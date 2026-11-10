use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::Duration;

fn serve<T: Send + 'static>(
    reply: impl FnOnce(TcpStream, String) -> T + Send + 'static,
) -> (String, JoinHandle<T>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        peer.set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            peer.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        reply(peer, String::from_utf8(request).unwrap())
    });
    (url, server)
}

#[test]
fn a_full_chunk_line_buffer_reports_a_stall_with_the_peer_open() {
    let (url, server) = serve(|mut peer, _| {
        peer.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n")
            .unwrap();
        peer.write_all(&vec![b'1'; 128 * 1024]).unwrap();
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    });
    let config = ureq::config::Config::builder()
        .proxy(None)
        .timeout_global(Some(Duration::from_secs(2)))
        .build();
    let agent = ureq::Agent::new_with_config(config);
    let mut response = agent.get(&url).call().unwrap();
    let error = response.body_mut().read_to_vec().unwrap_err();
    drop(response);
    drop(agent);
    server.join().unwrap();
    assert_eq!(error.to_string(), "body data reading stalled");
}

#[test]
fn eof_during_an_interim_response_does_not_send_the_body() {
    let (url, server) = serve(|mut peer, request| {
        assert!(
            request
                .to_ascii_lowercase()
                .contains("\r\nexpect: 100-continue\r\n")
        );
        peer.write_all(b"HTTP/1.1 100 Cont").unwrap();
        peer.shutdown(Shutdown::Write).unwrap();
        let mut body = Vec::new();
        peer.read_to_end(&mut body).unwrap();
        body
    });
    let config = ureq::config::Config::builder()
        .proxy(None)
        .timeout_global(Some(Duration::from_secs(2)))
        .timeout_await_100(Some(Duration::from_millis(100)))
        .build();
    let agent = ureq::Agent::new_with_config(config);
    let error = agent
        .post(&url)
        .header("expect", "100-continue")
        .send("hello")
        .unwrap_err();
    drop(agent);
    assert!(
        matches!(error, ureq::Error::Io(ref err) if err.kind() == std::io::ErrorKind::UnexpectedEof),
        "{error}"
    );
    assert!(
        server.join().unwrap().is_empty(),
        "request body sent after the response ended"
    );
}
