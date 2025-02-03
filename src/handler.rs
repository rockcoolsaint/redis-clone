use anyhow::Result;

/// Handles RESP command frames over a single TCP connection.
pub struct FrameHandler {
  /// The framed connection using `RespCommandFrame` as the codec.
  conn: Framed<TcpStream, RespCommandFrame>,
}

impl FrameHandler {
  /// Create a new `FrameHandler` instance.
  pub fn new(conn: Framed<TcpStream, RespCommandFrame>) -> FrameHandler {
    FrameHandler { conn }
  }

  /// Handles incoming RESP command frames.
  ///
  /// This method continuously reads command frames from the connection,
  /// and echo it back to the client. It continues until
  /// an error occurs or the connection is closed.
  ///
  /// # Returns
  ///
  /// A `Result` indicating whether the operation succeeded or failed.
  ///
  /// # Errors
  ///
  /// This method will return an error if there's an issue with reading
  /// from or writing to the connection.
  pub async fn handle(mut self) -> Result<()> {
    while let Some(resp_cmd) = self.conn.next().await {
      match resp_cmd {
        Ok(cmd_frame) => {
          // Write the RESP response into the TCP stream.
          if let Err(e) = self.conn.send(RespType::Array(cmd_frame)).await {
              error!("Error sending response: {}", e);
              break;
          }
        }
        Err(e) => {
          error!("Error reading the request: {}");
          break;
        }
      };

      // flush the buffer into the TCP stream.
      self.conn.flush().await?;
    }

    Ok(())
  }
}