use anyhow::Result;
use futures::{SinkExt, StreamExt};
use log::error;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::{
  command::Command,
  resp::{frame::RespCommandFrame, types::RespType},
  storage::db::DB,
};

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
  /// processes them, and sends back the responses. It continues until
  /// an error occurs or the connection is closed.
  ///
  /// # Arguments
  /// 
  /// * `db` - Reference to the database where the key-value pairs are stored.
  /// 
  /// # Returns
  ///
  /// A `Result` indicating whether the operation succeeded or failed.
  ///
  /// # Errors
  ///
  /// This method will return an error if there's an issue with reading
  /// from or writing to the connection.
  pub async fn handle(mut self, db: &DB) -> Result<()> {
    while let Some(resp_cmd) = self.conn.next().await {
      match resp_cmd {
        Ok(cmd_frame) => {
          // Read the command from the frame.
          let resp_cmd = Command::from_resp_command_frame(cmd_frame);

          // Execute the command and get the RESP response.
          // If command fails, return RESP SimpleError as response.
          let response = match resp_cmd {
              Ok(cmd) => cmd.execute(db),
              Err(e) => RespType::SimpleError(format!("{}", e)),
          };

          // Write the RESP response into the TCP stream.
          if let Err(e) = self.conn.send(response).await {
              error!("Error sending response: {}", e);
              break;
          }
        }
        Err(e) => {
          error!("Error reading the request: {}", e);
          break;
        }
      };

      // flush the buffer into the TCP stream.
      self.conn.flush().await?;
    }

    Ok(())
  }
}