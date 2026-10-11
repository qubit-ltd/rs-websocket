use qubit_websocket::WsClient;

#[test]
fn builder_rejects_invalid_subprotocol_tokens() {
    for subprotocols in [
        vec!["chat".to_owned(), "chat".to_owned()],
        vec!["chat,superchat".to_owned()],
        vec!["chat protocol".to_owned()],
        vec!["chat\nprotocol".to_owned()],
    ] {
        assert!(
            WsClient::builder()
                .subprotocols(subprotocols)
                .build()
                .is_err()
        );
    }
}
