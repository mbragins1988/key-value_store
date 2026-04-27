import grpc
import kvstore_pb2
import kvstore_pb2_grpc


def run_client():
    # Подключаемся к серверу
    channel = grpc.insecure_channel("localhost:8000")
    stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)

    # Тест Put
    print("1. Put key='name', value='Alice'")
    stub.Put(kvstore_pb2.PutRequest(key="name", value="Alice", ttl_seconds=0))

    # Тест Get
    print("\n2. Get key='name'")
    response = stub.Get(kvstore_pb2.GetRequest(key="name"))
    print(f"   Response: {response.value}")

    # Тест Put с TTL
    print("\n3. Put key='temp', value='temp_value', ttl=3 seconds")
    stub.Put(kvstore_pb2.PutRequest(key="temp", value="temp_value", ttl_seconds=3))

    # Тест List
    print("\n4. List all keys (prefix='')")
    response = stub.List(kvstore_pb2.ListRequest(prefix=""))
    for item in response.items:
        print(f"   {item.key} = {item.value}")

    # Тест Delete
    print("\n5. Delete key='name'")
    stub.Delete(kvstore_pb2.DeleteRequest(key="name"))

    # Проверяем, что удалилось
    print("\n6. Get key='name' after deletion")
    try:
        response = stub.Get(kvstore_pb2.GetRequest(key="name"))
    except grpc.RpcError as e:
        print(f"   Error (expected): {e.code()} - {e.details()}")

    # Тест TTL
    import time

    print("\n7. Waiting 4 seconds for TTL to expire...")
    time.sleep(4)

    try:
        response = stub.Get(kvstore_pb2.GetRequest(key="temp"))
    except grpc.RpcError as e:
        print(f"   Key 'temp' expired: {e.code()} - {e.details()}")


if __name__ == "__main__":
    run_client()
