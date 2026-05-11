import boto3

def test_conexao_aws():
    client = boto3.client("sts", region_name="us-east-1")
    response = client.get_caller_identity()
    print(f"\nConectado como: {response['Arn']}")
    print(f"ID da conta: {response['Account']}")
    assert response["Account"] is not None