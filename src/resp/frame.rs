use std::io::Error;

use bytes::Buf;
use tokio_util::codec::Decoder;

use crate::resp::types::RespType;

/// This codec handles Nimblecache commands, which are always represented
/// as array of bulk strings in the RESP (REdis Serialization Protocol) protocol.
///
/// The codec uses a `CommandBuilder` internally to construct the array of bulk strings
/// that make up a Nimblecache command.
pub struct RespCommandFrame {
  /// Builder for appending the bulk strings inthe command array.
  cmd_builder: Option<CommandBuilder>,
}

impl RespCommandFrame {
    /// Creates a new `RespCommandFrame`.
    ///
    /// # Returns
    ///
    /// A new instance of `RespCommandFrame` with no command builder initialized.
    pub fn new() -> RespCommandFrame {
      RespCommandFrame { cmd_builder: None }
    }
}

impl Decoder for RespCommandFrame {
    type Item = Vec<RespType>;

    type Error = std::io::Error;

    /// Decodes bytes from the input stream into a `Vec<RespType>` representing a Nimblecache command.
    ///
    /// This method implements the RESP protocol decoding logic, specifically handling
    /// arrays of bulk strings which represent Nimblecache commands. It uses a `CommandBuilder`
    /// to accumulate the parts of the command as they are received.
    ///
    /// # Arguments
    ///
    /// * `src` - A mutable reference to the input buffer containing bytes to decode.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Vec<RespType>))` if a complete command (array of bulk strings) was successfully decoded.
    /// * `Ok(None)` if more data is needed to complete the command.
    /// * `Err(std::io::Error)` if an error occurred during decoding.
    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // A command in RESP protocol should always be an array of Bulk Strings.
        // Check the first 2 bytes to validate if its a RESP array.
        if self.cmd_builder.is_none() {
          let (cmd_len, bytes_read) = match RespType::parse_array_len(src.clone()) {
              Ok(arr_len) => match arr_len {
                Some((len, bytes_read)) => (len, bytes_read),
                None => return Ok(None),
              },
              Err(e) => {
                  return Err(Error::new(
                    std::io::ErrorKind::InvalidData,
                    FrameError::from(e)
                  ));
              }
          };

          // initialize command builder, if its a valid RESP array.
          self.cmd_builder = Some(CommandBuilder::new(cmd_len));

          // advance buffer
          src.advance(bytes_read);
        }

        // Read all bytes in buffer
        while src.len() != 0 {
            // Validate and check the length of the next bulk string
            let (bullstr_len, bytes_read) = match RespType::parse_bulk_string_len(src.clone()) {
                Ok(bulkstr_len) => match bulkstr_len {
                    Some((len, bytes_read)) => (len, bytes_read),
                    None => return Ok(None),
                },
                Err(e) => {
                  return Err(Error::new(
                    std::io::ErrorKind::InvalidData,
                    FrameError::from(e),
                  ));
                }
            };

            // A bulk string has the below format
            //
            // `${string length in bytes }\r\n{string value}\r\n`
            //
            // Check if the buffer contains the required number of bytes to parse
            // the bulk string (including the CRLF at the end)
            let bulkstr_bytes = bullstr_len + bytes_read + 2;
            if src.len() < bulkstr_bytes {
              return Ok(None);;
            }

            // now that its sure the buffer has all the bytes required to parse the bulk string, parse it.
            let (bulkstr, bytes_read) = match RespType::parse_bulk_string(src.clone()) {
                Ok((resp_type, bytes_read)) => (resp_type, bytes_read),
                Err(e) => {
                    return Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        FrameError::from(e),
                    ));
                }
            };

            // append the bulk string to the command builder
            self.cmd_builder.as_mut().unwrap().add_part(bulkstr);

            // advance(bytes_read);
            src.advance(bytes_read);

            // if the command builder has all the parts, return it, else check buffer again
            let cmd_builder = self.cmd_builder.as_ref().unwrap();
            if cmd_builder.all_parts_received() {
              let cmd = cmd_builder.build();
              self.cmd_builder = None;
              return  Ok(Some(cmd));
            }
        }

        Ok(None)
    }
}