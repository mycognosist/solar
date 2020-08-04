extern crate jsonrpc_client_http;

use std::env;

use anyhow::Result;
use futures::SinkExt;
use jsonrpc_client_http::HttpTransport;

use crate::broker::*;

pub async fn actor(msg: String) -> Result<()> {
    let mut broker = BROKER.lock().await.register("oled", false).await?;

    oled_clear().unwrap();
    oled_write(0, 0, &msg, "6x8").unwrap();
    oled_flush().unwrap();

    /*
    let ctrlc = CtrlC::new().expect("cannot create Ctrl+C handler?");
    ctrlc.await;
    println!("Got CTRL-C, sending termination signal to jobs...");
    */
    let _ = broker.ch_broker.send(BrokerEvent::Terminate).await;

    Ok(())
}

fn oled_clear() -> Result<()> {
    let transport = HttpTransport::new().standalone().unwrap();
    let http_addr = env::var("PEACH_OLED_SERVER").unwrap_or_else(|_| "127.0.0.1:5112".to_string());
    let http_server = format!("http://{}", http_addr);
    let transport_handle = transport.handle(&http_server).unwrap();
    let mut client = PeachOledClient::new(transport_handle);

    client.clear().call().unwrap();

    Ok(())
}

fn oled_flush() -> Result<()> {
    let transport = HttpTransport::new().standalone().unwrap();
    let http_addr = env::var("PEACH_OLED_SERVER").unwrap_or_else(|_| "127.0.0.1:5112".to_string());
    let http_server = format!("http://{}", http_addr);
    let transport_handle = transport.handle(&http_server).unwrap();
    let mut client = PeachOledClient::new(transport_handle);

    client.flush().call().unwrap();

    Ok(())
}

fn oled_write(
    x_coord: i32,
    y_coord: i32,
    string: &str,
    font_size: &str,
) -> Result<()> {
    let transport = HttpTransport::new().standalone().unwrap();
    let http_addr = env::var("PEACH_OLED_SERVER").unwrap_or_else(|_| "127.0.0.1:5112".to_string());
    let http_server = format!("http://{}", http_addr);
    let transport_handle = transport.handle(&http_server).unwrap();
    let mut client = PeachOledClient::new(transport_handle);

    client.write(x_coord, y_coord, &string, &font_size).call().unwrap();

    Ok(())
}

jsonrpc_client!(pub struct PeachOledClient {
    /// Creates a JSON-RPC request to clear the OLED display.
    pub fn clear(&mut self) -> RpcRequest<String>;

    /// Creates a JSON-RPC request to flush the OLED display.
    pub fn flush(&mut self) -> RpcRequest<String>;

    /// Creates a JSON-RPC request to write to the OLED display.
    pub fn write(&mut self, x_coord: i32, y_coord: i32, string: &str, font_size: &str) -> RpcRequest<String>;
});
