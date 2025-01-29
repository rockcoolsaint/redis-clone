use anyhow::{Error, Result};
use bytes::BytesMut;
use log::error;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream}
};

use crate::resp::types::RespType;

#[derive(Debug)]
pub struct Server {
	listener: TcpListener
}

impl Server {
	/// Creates a new Server instance with the given TcpListener.
	pub fn new(listener:TcpListener) -> Server {
		Server { listener }
	}

	/// Runs the server in an infinite loop, continuously accepting and handling
    /// incoming connections.
	pub async fn run(&mut self) -> Result<()> {
		loop {
			// accept a new TCP connection.
            // If successful the corresponding TcpStream is stored
            // in the variable `sock`, else a panic will occur.
			let mut sock = match self.accept_conn().await {
				Ok(stream) => stream,
				// Log the error and panic if there is an issue accepting a connection.
				Err(e) => {
					error!("{}", e);
					panic!("Error accepting connection");
				}
			};

			// Spawn a new asynchronous task to handle the connection.
            // This allows the server to handle multiple connections concurrently.
			tokio::spawn(async move {
				// Write a "Hello!" message to the client.
				// read the TCP message and move the raw bytes into a buffer
				let mut buffer = BytesMut::with_capacity(512);
				if let Err(e) = sock.read_buf(&mut buffer).await {
					panic!("Error reading request: {}", e);
				}

				// Try parsing the RESP data from the bytes in the buffer.
				// If parsing fails return the error message as a RESP SimpleError data type.
				let resp_data = match RespType::parse(buffer) {
					Ok((data, _)) => data,
					Err(e) => RespType::SimpleError(format!("{}", e)),
				};

				// Echo the RESP message back to the client.
				if let Err(e) = &mut sock.write_all(&resp_data.to_bytes()[..]).await {
					// Log the error and panic if there is an issue writing the response.
					error!("{}", e);
					panic!("Error writing response")
				}
				// The connection is closed automatically when `sock` goes out of scope.
			});
		}
	}

	/// Accepts a new incoming TCP connection and returns the corresponding
    /// tokio TcpStream.
	async fn accept_conn(&mut self) -> Result<TcpStream> {
		loop {
			// Wait for an incoming connection.
            // The `accept()` method returns a tuple of (TcpStream, SocketAddr),
            // but we only need the TcpStream.
			match self.listener.accept().await {
				// Return the TcpStream if a connection is successfully accepted.
				Ok((sock, _)) => return Ok(sock),
				// Return an error if there is an issue accepting a connection.
				Err(e) => return Err(Error::from(e)),
			}
		}
	}
}