import grpc
from concurrent import futures
import time
import threading
from collections import OrderedDict
from typing import Dict, Optional, Tuple
import kvstore_pb2
import kvstore_pb2_grpc


class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        # Хранилище: ключ -> (значение, время_истечения)
        # time.time() + ttl_seconds
        self.store: Dict[str, Tuple[str, Optional[float]]] = {}

        # LRU порядок: OrderedDict хранит ключи в порядке использования
        self.lru_order = OrderedDict()

        # Максимальное количество ключей
        self.max_size = 10

        # Блокировка для потокобезопасности
        self.lock = threading.RLock()

        # Запускаем фоновую очистку просроченных ключей
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_expired, daemon=True
        )
        self.cleanup_thread.start()

    def _cleanup_expired(self):
        """Фоновый поток для очистки просроченных ключей"""
        while True:
            time.sleep(1)  # Проверяем каждую секунду
            with self.lock:
                current_time = time.time()
                expired_keys = []

                for key, (_, expiry) in self.store.items():
                    if expiry is not None and expiry <= current_time:
                        expired_keys.append(key)

                for key in expired_keys:
                    del self.store[key]
                    if key in self.lru_order:
                        del self.lru_order[key]

    def _check_expired(self, key: str) -> bool:
        """Проверяет, истек ли ключ"""
        if key not in self.store:
            return True

        value, expiry = self.store[key]
        if expiry is not None and expiry <= time.time():
            # Удаляем истекший ключ
            del self.store[key]
            if key in self.lru_order:
                del self.lru_order[key]
            return True

        return False

    def _update_lru(self, key: str):
        """Обновляет позицию ключа в LRU (перемещает в конец)"""
        if key in self.lru_order:
            self.lru_order.move_to_end(key)
        else:
            self.lru_order[key] = None

    def _evict_if_needed(self):
        """Удаляет наименее недавно использованный ключ если превышен лимит"""
        while len(self.store) >= self.max_size:
            # Удаляем первый (самый старый) ключ
            if self.lru_order:
                oldest_key = next(iter(self.lru_order))
                del self.store[oldest_key]
                del self.lru_order[oldest_key]

    def Put(self, request, context):
        """Добавить или обновить значение"""
        with self.lock:
            # Вычисляем время истечения
            expiry = None
            if request.ttl_seconds > 0:
                expiry = time.time() + request.ttl_seconds

            # Сохраняем значение
            self.store[request.key] = (request.value, expiry)

            # Обновляем LRU
            self._update_lru(request.key)

            # Проверяем лимит и вытесняем если нужно
            self._evict_if_needed()

            return kvstore_pb2.PutResponse()

    def Get(self, request, context):
        """Получить значение по ключу"""
        with self.lock:
            # Проверяем, не истек ли ключ
            if self._check_expired(request.key):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Key '{request.key}' not found or expired")
                return kvstore_pb2.GetResponse()

            # Получаем значение
            value, _ = self.store[request.key]

            # Обновляем LRU (чтение тоже считается использованием)
            self._update_lru(request.key)

            return kvstore_pb2.GetResponse(value=value)

    def Delete(self, request, context):
        """Удалить ключ"""
        with self.lock:
            if request.key in self.store:
                del self.store[request.key]
                if request.key in self.lru_order:
                    del self.lru_order[request.key]

            return kvstore_pb2.DeleteResponse()

    def List(self, request, context):
        """Вернуть все ключи с заданным префиксом"""
        with self.lock:
            items = []
            current_time = time.time()

            for key, (value, expiry) in list(self.store.items()):
                # Проверяем TTL
                if expiry is not None and expiry <= current_time:
                    # Пропускаем истекшие ключи
                    continue

                # Проверяем префикс
                if key.startswith(request.prefix):
                    items.append(kvstore_pb2.KeyValue(key=key, value=value))

            return kvstore_pb2.ListResponse(items=items)


def serve():
    """Запуск gRPC сервера"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(), server
    )

    # Слушаем порт 8000
    server.add_insecure_port("[::]:8000")
    server.start()
    print("Server started on port 8000")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.stop(0)


if __name__ == "__main__":
    serve()
