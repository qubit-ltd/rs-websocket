#[path = "fixtures/consumer/tests/contract/handshake_contract.rs"]
mod handshake_contract;

#[tokio::test]
async fn test_vendor_observes_full_connection_token_contract() {
    handshake_contract::assert_connection_contract(false).await;
}
