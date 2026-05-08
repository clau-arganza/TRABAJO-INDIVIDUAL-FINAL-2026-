"""
Black Friday Simulator
----------------------
Proyecto final de Programación Paralela y Distribuida en Python.

Simula una tienda online durante Black Friday usando concurrencia real:
- Productores: clientes que generan pedidos.
- Consumidores: servidores que procesan pedidos.
- Locks: protegen el stock y las estadísticas compartidas.
- Semaphore: limita los pagos simultáneos, como una pasarela de pago real.
- Event: coordina el cierre del sistema cuando ya no quedan pedidos.
- Queue/PriorityQueue: comunica clientes y servidores y da prioridad a clientes VIP.
- Comparación secuencial vs concurrente.
- Generación automática de un informe visual en HTML con gráficos SVG.

Mejoras añadidas:
- Tipos de cliente: normal, VIP y bot/reventa.
- Detector básico de bots para bloquear pedidos sospechosos.
- Ranking visual de productos más vendidos, más rentables y agotados.

Solo usa bibliotecas estándar de Python.
"""

from __future__ import annotations

import argparse
import html
import math
import os
import queue
import random
import statistics
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ============================================================
# CONFIGURACIÓN DEL SISTEMA
# ============================================================

SHOP_NAME = "CLICK&RUN BLACK FRIDAY"
DEFAULT_OUTPUT_DIR = "output_black_friday"

CLIENT_TYPES = {
    "VIP": {
        "label": "Cliente VIP",
        "icon": "⭐",
        "priority": 0,
        "probability": 0.13,
    },
    "NORMAL": {
        "label": "Cliente normal",
        "icon": "👤",
        "priority": 1,
        "probability": 0.82,
    },
    "BOT": {
        "label": "Bot/reventa",
        "icon": "🤖",
        "priority": 2,
        "probability": 0.05,
    },
}


@dataclass
class Product:
    """Producto vendido por la tienda."""

    name: str
    price: float
    stock: int
    popularity: float
    icon: str
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    initial_stock: int = field(init=False)
    sold: int = 0

    def __post_init__(self) -> None:
        self.initial_stock = self.stock

    def reset(self) -> None:
        with self.lock:
            self.stock = self.initial_stock
            self.sold = 0

    def try_buy_safe(self, quantity: int) -> bool:
        
        with self.lock:
            if self.stock >= quantity:
                # Pequeña pausa artificial para hacer visible el problema de concurrencia.
                # Al estar dentro del Lock, sigue siendo seguro.
                time.sleep(0.002)
                self.stock -= quantity
                self.sold += quantity
                return True
            return False

    def try_buy_unsafe(self, quantity: int) -> bool:
        """
        Versión insegura: NO usa Lock.
        Sirve para demostrar condiciones de carrera.
        """
        if self.stock >= quantity:
            time.sleep(0.002)
            self.stock -= quantity
            self.sold += quantity
            return True
        return False


@dataclass
class Order:
    """Pedido generado por un cliente."""

    order_id: int
    client_id: int
    client_type: str
    priority: int
    product_name: str
    quantity: int
    created_at: float


@dataclass
class OrderResult:
    """Resultado final de procesar un pedido."""

    order_id: int
    client_id: int
    client_type: str
    product_name: str
    quantity: int
    status: str
    revenue: float
    waiting_time: float
    processing_time: float
    worker_name: str
    message: str


@dataclass
class Stats:
    """Estadísticas globales protegidas con Lock."""

    accepted: int = 0
    rejected: int = 0
    failed_payments: int = 0
    bot_blocked: int = 0
    revenue: float = 0.0
    processed: int = 0
    results: List[OrderResult] = field(default_factory=list)
    client_type_counts: Dict[str, int] = field(
        default_factory=lambda: {client_type: 0 for client_type in CLIENT_TYPES}
    )
    client_type_accepted: Dict[str, int] = field(
        default_factory=lambda: {client_type: 0 for client_type in CLIENT_TYPES}
    )
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_result(self, result: OrderResult) -> None:
        with self.lock:
            self.processed += 1
            self.results.append(result)

            if result.client_type in self.client_type_counts:
                self.client_type_counts[result.client_type] += 1

            if result.status == "ACCEPTED":
                self.accepted += 1
                self.revenue += result.revenue
                if result.client_type in self.client_type_accepted:
                    self.client_type_accepted[result.client_type] += 1
            elif result.status == "PAYMENT_ERROR":
                self.failed_payments += 1
            elif result.status == "BOT_BLOCKED":
                self.bot_blocked += 1
            else:
                self.rejected += 1


@dataclass
class SimulationConfig:
    """Parámetros configurables de la simulación."""

    clients: int = 1200
    workers: int = 12
    payment_slots: int = 5
    max_quantity: int = 2
    unsafe: bool = False
    seed: Optional[int] = 7
    live: bool = True
    output_dir: str = DEFAULT_OUTPUT_DIR


# ============================================================
# DATOS DE LA TIENDA
# ============================================================


def build_products() -> Dict[str, Product]:
    """Crea el catálogo inicial de productos."""
    products = [
        Product("PlayStation 5", 499.99, 140, 0.18, "🎮"),
        Product("iPhone 16", 959.00, 90, 0.16, "📱"),
        Product("AirPods Pro", 249.00, 240, 0.15, "🎧"),
        Product("Nintendo Switch", 319.99, 180, 0.13, "🕹️"),
        Product("MacBook Air", 1199.00, 70, 0.10, "💻"),
        Product("Smart TV 55", 649.00, 110, 0.10, "📺"),
        Product("Kindle", 129.99, 220, 0.08, "📚"),
        Product("Robot Aspirador", 279.99, 150, 0.06, "🤖"),
        Product("Cafetera Deluxe", 189.99, 160, 0.04, "☕"),
    ]
    return {product.name: product for product in products}


# ============================================================
# UTILIDADES VISUALES
# ============================================================


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def money(value: float) -> str:
    return f"{value:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")


def pseudo_delay(seed: int, low: float, high: float) -> float:
    """Devuelve una pausa determinista para evitar depender de random dentro de hebras."""
    value = ((seed * 1103515245 + 12345) % 10_000) / 10_000
    return low + (high - low) * value


def payment_is_successful(order_id: int) -> bool:
    """Simula un fallo de pago determinista de aproximadamente el 4 %."""
    return ((order_id * 37 + 13) % 100) >= 4


def progress_bar(value: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "[" + " " * width + "]"
    filled = round((value / total) * width)
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def shop_banner() -> str:
    return r"""
╔════════════════════════════════════════════════════════════╗
║              TIENDA ONLINE EN BLACK FRIDAY                 ║
║                                                            ║
║         clientes masivos  →  cola  →  servidores           ║
║        VIP + bots + stock limitado + pagos simultáneos     ║
╚════════════════════════════════════════════════════════════╝
"""


def print_catalog(products: Dict[str, Product]) -> None:
    print("CATÁLOGO INICIAL")
    print("-" * 72)
    print(f"{'Producto':<24} {'Precio':>12} {'Stock':>8} {'Popularidad':>14}")
    print("-" * 72)
    for product in products.values():
        print(
            f"{product.icon} {product.name:<21} {money(product.price):>12} "
            f"{product.stock:>8} {product.popularity:>13.0%}"
        )
    print("-" * 72)
    print("Tipos de cliente simulados: 👤 normal | ⭐ VIP con prioridad | 🤖 bot/reventa")
    print("-" * 72)


def print_live_dashboard(
    stats: Stats,
    products: Dict[str, Product],
    total_orders: int,
    start_time: float,
    mode_name: str,
) -> None:
    """Dibuja un panel sencillo en terminal mientras se ejecuta la simulación."""
    elapsed = time.perf_counter() - start_time
    with stats.lock:
        processed = stats.processed
        accepted = stats.accepted
        rejected = stats.rejected
        failed = stats.failed_payments
        bots = stats.bot_blocked
        revenue = stats.revenue
        client_counts = dict(stats.client_type_counts)

    clear_screen()
    print(shop_banner())
    print(f"Modo: {mode_name}")
    print(f"Tiempo transcurrido: {elapsed:.2f} s")
    print(f"Pedidos procesados: {processed}/{total_orders}  {progress_bar(processed, total_orders)}")
    print(
        f"Aceptados: {accepted} | Rechazados por stock: {rejected} | "
        f"Fallos de pago: {failed} | Bots bloqueados: {bots}"
    )
    print(
        "Clientes procesados: "
        f"👤 {client_counts.get('NORMAL', 0)} | "
        f"⭐ {client_counts.get('VIP', 0)} | "
        f"🤖 {client_counts.get('BOT', 0)}"
    )
    print(f"Facturación acumulada: {money(revenue)}")
    print("\nSTOCK EN DIRECTO")
    print("-" * 72)
    for product in products.values():
        stock_left = max(product.stock, 0)
        bar = progress_bar(stock_left, product.initial_stock, width=24)
        print(
            f"{product.icon} {product.name:<21} {bar} "
            f"{stock_left:>3}/{product.initial_stock:<3} vendidos: {product.sold:<3}"
        )
    print("-" * 72)


# ============================================================
# GENERACIÓN DE PEDIDOS
# ============================================================


def choose_client_type() -> str:
    """Elige un tipo de cliente: normal, VIP o bot."""
    types = list(CLIENT_TYPES.keys())
    weights = [CLIENT_TYPES[client_type]["probability"] for client_type in types]
    return random.choices(types, weights=weights, k=1)[0]


def choose_product(products: Dict[str, Product], client_type: str) -> str:
    """Elige un producto ponderando por popularidad y tipo de cliente."""
    names = list(products.keys())

    if client_type == "BOT":
        # Los bots intentan ir a productos caros y populares para reventa.
        weights = [
            products[name].popularity * (1 + products[name].price / 1000)
            for name in names
        ]
    elif client_type == "VIP":
        # Los VIP tienen una ligera preferencia por productos de mayor valor.
        weights = [
            products[name].popularity * (1 + products[name].price / 2500)
            for name in names
        ]
    else:
        weights = [products[name].popularity for name in names]

    return random.choices(names, weights=weights, k=1)[0]


def choose_quantity(config: SimulationConfig, client_type: str) -> int:
    """Decide cuántas unidades pide cada tipo de cliente."""
    if client_type == "BOT":
        # Los bots suelen intentar comprar el máximo permitido.
        if random.random() < 0.75:
            return config.max_quantity
    elif client_type == "VIP":
        # Los VIP normalmente compran una unidad, pero a veces compran dos.
        if config.max_quantity >= 2 and random.random() < 0.25:
            return 2
        return 1

    return random.randint(1, config.max_quantity)


def build_orders(config: SimulationConfig, products: Dict[str, Product]) -> List[Order]:
    """Genera todos los pedidos iniciales."""
    if config.seed is not None:
        random.seed(config.seed)

    orders: List[Order] = []
    base_time = time.perf_counter()
    for order_id in range(1, config.clients + 1):
        client_type = choose_client_type()
        product_name = choose_product(products, client_type)
        quantity = choose_quantity(config, client_type)
        orders.append(
            Order(
                order_id=order_id,
                client_id=10_000 + order_id,
                client_type=client_type,
                priority=int(CLIENT_TYPES[client_type]["priority"]),
                product_name=product_name,
                quantity=quantity,
                created_at=base_time,
            )
        )
    return orders


# ============================================================
# DETECTOR DE BOTS
# ============================================================


def is_suspicious_bot(order: Order, product: Product) -> bool:
    """
    Detector básico de bots.

    La idea no es hacer ciberseguridad real, sino simular una regla de negocio:
    en Black Friday algunos pedidos automatizados intentan comprar muchas unidades
    de productos caros o muy demandados para reventa.

    Se bloquea un pedido cuando acumula suficientes señales sospechosas.
    """
    if order.client_type != "BOT":
        return False

    score = 0

    if order.quantity >= 2:
        score += 2
    if product.price >= 450:
        score += 1
    if product.popularity >= 0.12:
        score += 1
    if order.order_id % 3 != 0:
        # Simula que algunos bots llegan con patrones repetitivos detectables.
        score += 1

    return score >= 3


# ============================================================
# SIMULACIÓN CONCURRENTE
# ============================================================


QueueItem = Tuple[int, int, Optional[Order]]


def client_producer(
    client_orders: List[Order],
    order_queue: "queue.PriorityQueue[QueueItem]",
    producer_done_counter: List[int],
    producer_lock: threading.Lock,
    producers_done_event: threading.Event,
    total_producers: int,
) -> None:
    """
    Productor: mete pedidos en la cola.

    Hay varios productores para simular que muchos clientes llegan a la tienda
    online al mismo tiempo. Se usa PriorityQueue para que los VIP tengan prioridad.
    """
    for order in client_orders:
        # Pequeño retardo para simular clientes entrando en momentos ligeramente distintos.
        time.sleep(pseudo_delay(order.order_id, 0.002, 0.012))
        order.created_at = time.perf_counter()
        order_queue.put((order.priority, order.order_id, order))

    with producer_lock:
        producer_done_counter[0] += 1
        if producer_done_counter[0] >= total_producers:
            producers_done_event.set()


def payment_gateway(payment_slots: threading.Semaphore, order_id: int) -> bool:
    """
    Simula la pasarela de pago.

    El Semaphore limita cuántos pagos pueden procesarse a la vez.
    """
    with payment_slots:
        time.sleep(pseudo_delay(order_id, 0.006, 0.025))
        # Pequeña probabilidad de fallo de pago realista.
        return payment_is_successful(order_id)


def worker_consumer(
    worker_id: int,
    products: Dict[str, Product],
    order_queue: "queue.PriorityQueue[QueueItem]",
    stats: Stats,
    payment_slots: threading.Semaphore,
    unsafe: bool,
) -> None:
    """
    Consumidor: saca pedidos de la cola y los procesa.

    El trabajador termina cuando recibe una orden None. Este patrón de
    "sentinela" evita dejar hebras esperando indefinidamente.
    """
    worker_name = f"Servidor-{worker_id}"

    while True:
        _, _, order = order_queue.get()
        if order is None:
            order_queue.task_done()
            break

        start_processing = time.perf_counter()
        product = products[order.product_name]

        if is_suspicious_bot(order, product):
            time.sleep(pseudo_delay(order.order_id, 0.001, 0.004))
            result = OrderResult(
                order_id=order.order_id,
                client_id=order.client_id,
                client_type=order.client_type,
                product_name=order.product_name,
                quantity=order.quantity,
                status="BOT_BLOCKED",
                revenue=0.0,
                waiting_time=start_processing - order.created_at,
                processing_time=time.perf_counter() - start_processing,
                worker_name=worker_name,
                message="Pedido bloqueado por actividad sospechosa de bot/reventa.",
            )
            stats.add_result(result)
            order_queue.task_done()
            continue

        if unsafe:
            bought = product.try_buy_unsafe(order.quantity)
        else:
            bought = product.try_buy_safe(order.quantity)

        if not bought:
            result = OrderResult(
                order_id=order.order_id,
                client_id=order.client_id,
                client_type=order.client_type,
                product_name=order.product_name,
                quantity=order.quantity,
                status="NO_STOCK",
                revenue=0.0,
                waiting_time=start_processing - order.created_at,
                processing_time=time.perf_counter() - start_processing,
                worker_name=worker_name,
                message="Pedido rechazado: stock insuficiente.",
            )
        else:
            payment_ok = payment_gateway(payment_slots, order.order_id)
            if payment_ok:
                result = OrderResult(
                    order_id=order.order_id,
                    client_id=order.client_id,
                    client_type=order.client_type,
                    product_name=order.product_name,
                    quantity=order.quantity,
                    status="ACCEPTED",
                    revenue=product.price * order.quantity,
                    waiting_time=start_processing - order.created_at,
                    processing_time=time.perf_counter() - start_processing,
                    worker_name=worker_name,
                    message="Compra completada correctamente.",
                )
            else:
                # Si el pago falla, devolvemos el stock. También debe hacerse protegido.
                with product.lock:
                    product.stock += order.quantity
                    product.sold -= order.quantity
                result = OrderResult(
                    order_id=order.order_id,
                    client_id=order.client_id,
                    client_type=order.client_type,
                    product_name=order.product_name,
                    quantity=order.quantity,
                    status="PAYMENT_ERROR",
                    revenue=0.0,
                    waiting_time=start_processing - order.created_at,
                    processing_time=time.perf_counter() - start_processing,
                    worker_name=worker_name,
                    message="Pedido cancelado: fallo en la pasarela de pago.",
                )

        stats.add_result(result)
        order_queue.task_done()


def run_concurrent_simulation(
    config: SimulationConfig,
    products: Dict[str, Product],
    orders: List[Order],
) -> Tuple[Stats, float]:
    """Ejecuta la simulación concurrente."""
    stats = Stats()
    order_queue: "queue.PriorityQueue[QueueItem]" = queue.PriorityQueue()
    payment_slots = threading.Semaphore(config.payment_slots)
    producers_done_event = threading.Event()

    producer_count = min(6, max(2, math.ceil(config.clients / 25)))
    producer_done_counter = [0]
    producer_lock = threading.Lock()
    chunks = split_orders(orders, producer_count)
    producers: List[threading.Thread] = []
    workers: List[threading.Thread] = []

    start = time.perf_counter()

    # Primero arrancan los servidores, que se quedan esperando pedidos en la cola.
    for worker_id in range(1, config.workers + 1):
        thread = threading.Thread(
            target=worker_consumer,
            name=f"Servidor-{worker_id}",
            args=(
                worker_id,
                products,
                order_queue,
                stats,
                payment_slots,
                config.unsafe,
            ),
        )
        thread.start()
        workers.append(thread)

    # Después arrancan los productores, que representan clientes generando pedidos.
    for idx, chunk in enumerate(chunks, start=1):
        thread = threading.Thread(
            target=client_producer,
            name=f"ClienteProductor-{idx}",
            args=(
                chunk,
                order_queue,
                producer_done_counter,
                producer_lock,
                producers_done_event,
                producer_count,
            ),
        )
        thread.start()
        producers.append(thread)

    # Monitor visual mientras los productores o consumidores siguen trabajando.
    while True:
        all_producers_finished = all(thread.is_alive() is False for thread in producers)
        with stats.lock:
            all_orders_processed = stats.processed >= len(orders)
        if all_producers_finished and all_orders_processed:
            break
        if config.live:
            mode_name = "CONCURRENTE SIN LOCK" if config.unsafe else "CONCURRENTE SEGURA"
            print_live_dashboard(stats, products, len(orders), start, mode_name)
            time.sleep(0.15)
        else:
            time.sleep(0.05)

    for thread in producers:
        thread.join()

    # Espera a que todos los pedidos reales hayan sido procesados.
    order_queue.join()

    # Enviamos una señal de parada a cada servidor.
    for _ in workers:
        order_queue.put((9999, 9999, None))

    # Esperamos a que los servidores lean su señal de parada.
    for thread in workers:
        thread.join()

    elapsed = time.perf_counter() - start
    if config.live:
        mode_name = "CONCURRENTE SIN LOCK" if config.unsafe else "CONCURRENTE SEGURA"
        print_live_dashboard(stats, products, len(orders), start, mode_name)
    return stats, elapsed


# ============================================================
# SIMULACIÓN SECUENCIAL
# ============================================================


def run_sequential_simulation(
    products: Dict[str, Product],
    orders: List[Order],
) -> Tuple[Stats, float]:
    """Ejecuta una versión secuencial para comparar rendimiento."""
    stats = Stats()
    start = time.perf_counter()

    # Se ordena por prioridad para aplicar la misma lógica de negocio que en PriorityQueue.
    for order in sorted(orders, key=lambda item: (item.priority, item.order_id)):
        order.created_at = time.perf_counter()
        start_processing = time.perf_counter()
        product = products[order.product_name]

        if is_suspicious_bot(order, product):
            time.sleep(pseudo_delay(order.order_id, 0.001, 0.004))
            result = OrderResult(
                order_id=order.order_id,
                client_id=order.client_id,
                client_type=order.client_type,
                product_name=order.product_name,
                quantity=order.quantity,
                status="BOT_BLOCKED",
                revenue=0.0,
                waiting_time=0.0,
                processing_time=time.perf_counter() - start_processing,
                worker_name="Secuencial",
                message="Pedido bloqueado por actividad sospechosa de bot/reventa.",
            )
            stats.add_result(result)
            continue

        # En secuencial no hace falta Lock porque solo hay un flujo de ejecución.
        if product.stock >= order.quantity:
            time.sleep(0.002)
            product.stock -= order.quantity
            product.sold += order.quantity
            time.sleep(pseudo_delay(order.order_id, 0.006, 0.025))
            payment_ok = payment_is_successful(order.order_id)
            if payment_ok:
                result = OrderResult(
                    order_id=order.order_id,
                    client_id=order.client_id,
                    client_type=order.client_type,
                    product_name=order.product_name,
                    quantity=order.quantity,
                    status="ACCEPTED",
                    revenue=product.price * order.quantity,
                    waiting_time=0.0,
                    processing_time=time.perf_counter() - start_processing,
                    worker_name="Secuencial",
                    message="Compra completada correctamente.",
                )
            else:
                product.stock += order.quantity
                product.sold -= order.quantity
                result = OrderResult(
                    order_id=order.order_id,
                    client_id=order.client_id,
                    client_type=order.client_type,
                    product_name=order.product_name,
                    quantity=order.quantity,
                    status="PAYMENT_ERROR",
                    revenue=0.0,
                    waiting_time=0.0,
                    processing_time=time.perf_counter() - start_processing,
                    worker_name="Secuencial",
                    message="Pedido cancelado: fallo en la pasarela de pago.",
                )
        else:
            result = OrderResult(
                order_id=order.order_id,
                client_id=order.client_id,
                client_type=order.client_type,
                product_name=order.product_name,
                quantity=order.quantity,
                status="NO_STOCK",
                revenue=0.0,
                waiting_time=0.0,
                processing_time=time.perf_counter() - start_processing,
                worker_name="Secuencial",
                message="Pedido rechazado: stock insuficiente.",
            )

        stats.add_result(result)

    elapsed = time.perf_counter() - start
    return stats, elapsed


# ============================================================
# DEMO DE CONDICIÓN DE CARRERA
# ============================================================


def race_condition_demo() -> str:
    """
    Ejecuta una mini demostración insegura para que el informe pueda explicar
    por qué el Lock es necesario.
    """
    demo_product = Product("Producto Estrella", 100.0, 5, 1.0, "🔥")
    successes = []
    threads = []

    def unsafe_buyer(buyer_id: int) -> None:
        bought = demo_product.try_buy_unsafe(1)
        successes.append((buyer_id, bought))

    for buyer_id in range(1, 16):
        t = threading.Thread(target=unsafe_buyer, args=(buyer_id,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    bought_count = sum(1 for _, ok in successes if ok)
    oversold_units = max(0, bought_count - demo_product.initial_stock)

    return (
        f"En la demo insegura había {demo_product.initial_stock} unidades y "
        f"{bought_count} compradores consiguieron comprar. "
        f"Sobreventa detectada: {oversold_units} unidades. "
        f"Stock final: {demo_product.stock}."
    )


# ============================================================
# INFORME VISUAL
# ============================================================


def escape(value: object) -> str:
    return html.escape(str(value))


def svg_bar_chart(data: List[Tuple[str, float]], title: str, suffix: str = "") -> str:
    """Genera un gráfico de barras SVG sin dependencias externas."""
    width = 920
    row_height = 42
    margin_left = 240
    margin_right = 80
    margin_top = 70
    chart_width = width - margin_left - margin_right
    height = margin_top + row_height * len(data) + 40
    max_value = max((value for _, value in data), default=1)
    max_value = max(max_value, 1)

    rows = []
    for index, (label, value) in enumerate(data):
        y = margin_top + index * row_height
        bar_width = (value / max_value) * chart_width
        rows.append(
            f'''
            <text x="20" y="{y + 24}" font-size="15">{escape(label)}</text>
            <rect x="{margin_left}" y="{y}" width="{bar_width:.2f}" height="26" rx="8"></rect>
            <text x="{margin_left + bar_width + 10}" y="{y + 19}" font-size="14">{value:.0f}{escape(suffix)}</text>
            '''
        )

    return f'''
    <svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">
        <style>
            svg {{ max-width: 100%; height: auto; }}
            rect {{ fill: #111827; opacity: 0.88; }}
            text {{ font-family: Arial, sans-serif; fill: #111827; }}
            .title {{ font-size: 22px; font-weight: 700; }}
        </style>
        <text x="20" y="36" class="title">{escape(title)}</text>
        {''.join(rows)}
    </svg>
    '''


def svg_donut(accepted: int, rejected: int, failed: int, bot_blocked: int) -> str:
    """Genera un gráfico circular tipo donut con SVG."""
    total = max(accepted + rejected + failed + bot_blocked, 1)
    values = [accepted, rejected, failed, bot_blocked]
    labels = ["Aceptados", "Sin stock", "Pago fallido", "Bots bloqueados"]
    colors = ["#111827", "#6b7280", "#d1d5db", "#ef4444"]
    radius = 90
    circumference = 2 * math.pi * radius
    offset = 0
    circles = []

    for value, color in zip(values, colors):
        dash = circumference * (value / total)
        circles.append(
            f'<circle r="{radius}" cx="150" cy="150" fill="transparent" stroke="{color}" '
            f'stroke-width="32" stroke-dasharray="{dash:.2f} {circumference - dash:.2f}" '
            f'stroke-dashoffset="{-offset:.2f}" transform="rotate(-90 150 150)" />'
        )
        offset += dash

    legend = []
    for idx, (label, value, color) in enumerate(zip(labels, values, colors)):
        y = 62 + idx * 36
        pct = value / total * 100
        legend.append(
            f'<rect x="300" y="{y - 16}" width="18" height="18" rx="4" fill="{color}" />'
            f'<text x="330" y="{y}" font-size="16">{escape(label)}: {value} ({pct:.1f}%)</text>'
        )

    return f'''
    <svg viewBox="0 0 660 310" role="img" aria-label="Resumen de pedidos">
        <style>
            text {{ font-family: Arial, sans-serif; fill: #111827; }}
            .big {{ font-size: 28px; font-weight: 700; }}
            .small {{ font-size: 14px; fill: #4b5563; }}
        </style>
        {''.join(circles)}
        <text x="150" y="143" text-anchor="middle" class="big">{total}</text>
        <text x="150" y="166" text-anchor="middle" class="small">pedidos</text>
        {''.join(legend)}
    </svg>
    '''


def write_csv(path: Path, results: List[OrderResult]) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(
            "order_id,client_id,client_type,product_name,quantity,status,revenue,waiting_time,"
            "processing_time,worker_name,message\n"
        )
        for r in results:
            safe_message = r.message.replace('"', "'")
            f.write(
                f'{r.order_id},{r.client_id},{r.client_type},"{r.product_name}",{r.quantity},'
                f'{r.status},{r.revenue:.2f},{r.waiting_time:.5f},'
                f'{r.processing_time:.5f},{r.worker_name},"{safe_message}"\n'
            )


def product_revenue_ranking(products: Dict[str, Product]) -> List[Tuple[str, float]]:
    """Calcula ingresos por producto a partir de unidades vendidas y precio."""
    data = [
        (f"{product.icon} {product.name}", product.sold * product.price)
        for product in products.values()
    ]
    return sorted(data, key=lambda item: item[1], reverse=True)


def product_sold_ranking(products: Dict[str, Product]) -> List[Tuple[str, float]]:
    """Calcula ranking de unidades vendidas."""
    data = [(f"{product.icon} {product.name}", product.sold) for product in products.values()]
    return sorted(data, key=lambda item: item[1], reverse=True)


def sold_out_products(products: Dict[str, Product]) -> List[Product]:
    """Devuelve productos agotados."""
    return [product for product in products.values() if product.stock <= 0]


def generate_report(
    output_dir: Path,
    concurrent_stats: Stats,
    concurrent_time: float,
    sequential_stats: Stats,
    sequential_time: float,
    products: Dict[str, Product],
    config: SimulationConfig,
    race_message: str,
) -> Path:
    """Genera un informe visual HTML con tablas y gráficos."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "pedidos_resultados.csv"
    write_csv(csv_path, concurrent_stats.results)

    average_wait = statistics.mean([r.waiting_time for r in concurrent_stats.results]) if concurrent_stats.results else 0.0
    max_wait = max([r.waiting_time for r in concurrent_stats.results], default=0.0)
    average_processing = statistics.mean([r.processing_time for r in concurrent_stats.results]) if concurrent_stats.results else 0.0
    speedup = sequential_time / concurrent_time if concurrent_time > 0 else 0.0

    sold_data = [(f"{p.icon} {p.name}", p.sold) for p in products.values()]
    stock_data = [(f"{p.icon} {p.name}", max(p.stock, 0)) for p in products.values()]
    top_sold_data = product_sold_ranking(products)[:5]
    top_revenue_data = product_revenue_ranking(products)[:5]
    client_data = [
        (f"{CLIENT_TYPES['NORMAL']['icon']} Normal", concurrent_stats.client_type_counts.get("NORMAL", 0)),
        (f"{CLIENT_TYPES['VIP']['icon']} VIP", concurrent_stats.client_type_counts.get("VIP", 0)),
        (f"{CLIENT_TYPES['BOT']['icon']} Bot/reventa", concurrent_stats.client_type_counts.get("BOT", 0)),
    ]

    accepted_client_rows = "".join(
        f"""
        <tr>
            <td>{CLIENT_TYPES[client_type]["icon"]} {escape(CLIENT_TYPES[client_type]["label"])}</td>
            <td>{concurrent_stats.client_type_counts.get(client_type, 0)}</td>
            <td>{concurrent_stats.client_type_accepted.get(client_type, 0)}</td>
        </tr>
        """
        for client_type in ["NORMAL", "VIP", "BOT"]
    )

    recent_rows = "".join(
        f"""
        <tr>
            <td>{r.order_id}</td>
            <td>{CLIENT_TYPES.get(r.client_type, {}).get("icon", "")} {escape(r.client_type)}</td>
            <td>{escape(r.product_name)}</td>
            <td>{r.quantity}</td>
            <td><span class="badge {escape(r.status.lower())}">{escape(r.status)}</span></td>
            <td>{money(r.revenue)}</td>
            <td>{r.worker_name}</td>
        </tr>
        """
        for r in concurrent_stats.results[:25]
    )

    product_rows = "".join(
        f"""
        <tr>
            <td>{p.icon} {escape(p.name)}</td>
            <td>{money(p.price)}</td>
            <td>{p.initial_stock}</td>
            <td>{p.sold}</td>
            <td>{p.stock}</td>
            <td>{money(p.sold * p.price)}</td>
        </tr>
        """
        for p in products.values()
    )

    sold_out_list = sold_out_products(products)
    sold_out_html = (
        "<ul>" + "".join(f"<li>{p.icon} {escape(p.name)}</li>" for p in sold_out_list) + "</ul>"
        if sold_out_list
        else "<p>No se agotó ningún producto en esta ejecución.</p>"
    )

    report_path = output_dir / "informe_visual_black_friday.html"
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    html_doc = f"""
<!doctype html>
<html lang="es">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Informe visual - Black Friday Simulator</title>
    <style>
        body {{
            margin: 0;
            font-family: Arial, sans-serif;
            background: #f3f4f6;
            color: #111827;
        }}
        header {{
            background: linear-gradient(135deg, #111827, #374151);
            color: white;
            padding: 36px 28px;
        }}
        header h1 {{ margin: 0 0 8px; font-size: 38px; }}
        header p {{ margin: 0; opacity: 0.9; font-size: 17px; }}
        main {{ max-width: 1100px; margin: 0 auto; padding: 26px; }}
        .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; }}
        .card {{
            background: white;
            border-radius: 18px;
            padding: 20px;
            box-shadow: 0 12px 28px rgba(15,23,42,0.08);
            margin-bottom: 18px;
        }}
        .metric {{ font-size: 28px; font-weight: 800; margin-top: 8px; }}
        .label {{ font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: .08em; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 11px; border-bottom: 1px solid #e5e7eb; text-align: left; }}
        th {{ font-size: 13px; color: #4b5563; text-transform: uppercase; letter-spacing: .06em; }}
        .badge {{ display: inline-block; padding: 5px 9px; border-radius: 999px; font-size: 12px; font-weight: 700; }}
        .accepted {{ background: #dcfce7; color: #166534; }}
        .no_stock {{ background: #fee2e2; color: #991b1b; }}
        .payment_error {{ background: #fef3c7; color: #92400e; }}
        .bot_blocked {{ background: #fee2e2; color: #7f1d1d; }}
        .two {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }}
        .three {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 18px; }}
        .note {{ background: #fffbeb; border: 1px solid #fde68a; }}
        code {{ background: #f3f4f6; padding: 2px 5px; border-radius: 6px; }}
        footer {{ color: #6b7280; padding: 20px 0 40px; }}
        @media (max-width: 900px) {{ .grid, .two, .three {{ grid-template-columns: 1fr; }} }}
    </style>
</head>
<body>
    <header>
        <h1> Black Friday Simulator</h1>
        <p>Sistema concurrente de compras, stock, clientes VIP, bots y pagos en Python</p>
    </header>
    <main>
        <section class="grid">
            <div class="card"><div class="label">Pedidos aceptados</div><div class="metric">{concurrent_stats.accepted}</div></div>
            <div class="card"><div class="label">Rechazados por stock</div><div class="metric">{concurrent_stats.rejected}</div></div>
            <div class="card"><div class="label">Bots bloqueados</div><div class="metric">{concurrent_stats.bot_blocked}</div></div>
            <div class="card"><div class="label">Aceleración</div><div class="metric">x{speedup:.2f}</div></div>
        </section>

        <section class="grid">
            <div class="card"><div class="label">Facturación</div><div class="metric">{money(concurrent_stats.revenue)}</div></div>
            <div class="card"><div class="label">Clientes normales</div><div class="metric">{concurrent_stats.client_type_counts.get("NORMAL", 0)}</div></div>
            <div class="card"><div class="label">Clientes VIP</div><div class="metric">{concurrent_stats.client_type_counts.get("VIP", 0)}</div></div>
            <div class="card"><div class="label">Bots/reventa</div><div class="metric">{concurrent_stats.client_type_counts.get("BOT", 0)}</div></div>
        </section>

        <section class="two">
            <div class="card">
                <h2>Resumen de pedidos</h2>
                {svg_donut(concurrent_stats.accepted, concurrent_stats.rejected, concurrent_stats.failed_payments, concurrent_stats.bot_blocked)}
            </div>
            <div class="card">
                <h2>Rendimiento</h2>
                <table>
                    <tr><th>Versión</th><th>Tiempo</th><th>Pedidos</th></tr>
                    <tr><td>Secuencial</td><td>{sequential_time:.3f} s</td><td>{sequential_stats.processed}</td></tr>
                    <tr><td>Concurrente</td><td>{concurrent_time:.3f} s</td><td>{concurrent_stats.processed}</td></tr>
                </table>
                <p>Tiempo medio de espera en cola: <strong>{average_wait:.4f} s</strong></p>
                <p>Tiempo máximo de espera en cola: <strong>{max_wait:.4f} s</strong></p>
                <p>Tiempo medio de procesamiento: <strong>{average_processing:.4f} s</strong></p>
            </div>
        </section>

        <section class="two">
            <div class="card">
                <h2>Tipos de cliente</h2>
                {svg_bar_chart(client_data, "Pedidos procesados por tipo de cliente")}
            </div>
            <div class="card">
                <h2>Pedidos aceptados por tipo</h2>
                <table>
                    <tr><th>Tipo de cliente</th><th>Procesados</th><th>Aceptados</th></tr>
                    {accepted_client_rows}
                </table>
                <p>Los clientes VIP entran con mayor prioridad en la <code>PriorityQueue</code>. Los bots se analizan antes de consumir stock.</p>
            </div>
        </section>

        <section class="three">
            <div class="card">
                <h2>🏆 Producto más vendido</h2>
                <p class="metric">{escape(top_sold_data[0][0])}</p>
                <p>{top_sold_data[0][1]:.0f} unidades vendidas</p>
            </div>
            <div class="card">
                <h2>💰 Producto más rentable</h2>
                <p class="metric">{escape(top_revenue_data[0][0])}</p>
                <p>{money(top_revenue_data[0][1])} generados</p>
            </div>
            <div class="card">
                <h2>🚨 Productos agotados</h2>
                {sold_out_html}
            </div>
        </section>

        <section class="card">
            <h2>Ranking de productos más vendidos</h2>
            {svg_bar_chart(top_sold_data, "Top 5 por unidades vendidas")}
        </section>

        <section class="card">
            <h2>Ranking de productos más rentables</h2>
            {svg_bar_chart(top_revenue_data, "Top 5 por facturación aproximada", " €")}
        </section>

        <section class="card">
            <h2>Productos vendidos</h2>
            {svg_bar_chart(sold_data, "Unidades vendidas por producto")}
        </section>

        <section class="card">
            <h2>Stock restante</h2>
            {svg_bar_chart(stock_data, "Stock final por producto")}
        </section>

        <section class="card note">
            <h2>Demostración de condición de carrera</h2>
            <p>{escape(race_message)}</p>
            <p>Esta prueba justifica el uso de <code>Lock</code>: sin exclusión mutua, varias hebras pueden leer el mismo stock antes de que otra lo actualice.</p>
        </section>

        <section class="card">
            <h2>Catálogo final</h2>
            <table>
                <tr><th>Producto</th><th>Precio</th><th>Stock inicial</th><th>Vendido</th><th>Stock final</th><th>Facturación aprox.</th></tr>
                {product_rows}
            </table>
        </section>

        <section class="card">
            <h2>Primeros pedidos procesados</h2>
            <table>
                <tr><th>ID</th><th>Cliente</th><th>Producto</th><th>Cantidad</th><th>Estado</th><th>Importe</th><th>Servidor</th></tr>
                {recent_rows}
            </table>
            <p>El listado completo está en <code>{csv_path.name}</code>.</p>
        </section>

        <section class="card">
            <h2>Configuración usada</h2>
            <table>
                <tr><th>Parámetro</th><th>Valor</th></tr>
                <tr><td>Clientes simulados</td><td>{config.clients}</td></tr>
                <tr><td>Servidores concurrentes</td><td>{config.workers}</td></tr>
                <tr><td>Pagos simultáneos permitidos</td><td>{config.payment_slots}</td></tr>
                <tr><td>Cantidad máxima por pedido</td><td>{config.max_quantity}</td></tr>
                <tr><td>Modo inseguro</td><td>{config.unsafe}</td></tr>
            </table>
        </section>

        <footer>
            Informe generado automáticamente el {now}.
        </footer>
    </main>
</body>
</html>
"""
    report_path.write_text(html_doc, encoding="utf-8")
    return report_path


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================


def split_orders(orders: List[Order], chunks: int) -> List[List[Order]]:
    result = [[] for _ in range(chunks)]
    for index, order in enumerate(orders):
        result[index % chunks].append(order)
    return result


def clone_products(products: Dict[str, Product]) -> Dict[str, Product]:
    return {
        name: Product(
            name=product.name,
            price=product.price,
            stock=product.initial_stock,
            popularity=product.popularity,
            icon=product.icon,
        )
        for name, product in products.items()
    }


def print_ranking(products: Dict[str, Product]) -> None:
    """Imprime rankings principales en terminal."""
    sold_ranking = product_sold_ranking(products)
    revenue_ranking = product_revenue_ranking(products)
    sold_out = sold_out_products(products)

    print("\nRANKING VISUAL")
    print("-" * 72)

    print("Top 3 productos más vendidos:")
    for index, (label, units) in enumerate(sold_ranking[:3], start=1):
        print(f"{index}. {label}: {units:.0f} unidades")

    print("\nTop 3 productos más rentables:")
    for index, (label, revenue) in enumerate(revenue_ranking[:3], start=1):
        print(f"{index}. {label}: {money(revenue)}")

    if sold_out:
        agotados = ", ".join(f"{product.icon} {product.name}" for product in sold_out)
        print(f"\nProductos agotados: {agotados}")
    else:
        print("\nProductos agotados: ninguno")


def print_final_summary(
    concurrent_stats: Stats,
    concurrent_time: float,
    sequential_stats: Stats,
    sequential_time: float,
    report_path: Path,
    config: SimulationConfig,
    products: Dict[str, Product],
) -> None:
    speedup = sequential_time / concurrent_time if concurrent_time > 0 else 0.0
    print("\n" + "=" * 72)
    print("RESUMEN FINAL")
    print("=" * 72)
    print(f"Pedidos simulados: {config.clients}")
    print(f"Servidores concurrentes: {config.workers}")
    print(f"Pagos simultáneos permitidos: {config.payment_slots}")
    print("-" * 72)
    print(f"Versión secuencial:   {sequential_time:.3f} s")
    print(f"Versión concurrente:  {concurrent_time:.3f} s")
    print(f"Aceleración obtenida: x{speedup:.2f}")
    print("-" * 72)
    print(f"Pedidos aceptados:    {concurrent_stats.accepted}")
    print(f"Rechazados sin stock: {concurrent_stats.rejected}")
    print(f"Fallos de pago:       {concurrent_stats.failed_payments}")
    print(f"Bots bloqueados:      {concurrent_stats.bot_blocked}")
    print(f"Facturación final:    {money(concurrent_stats.revenue)}")
    print("-" * 72)
    print("Tipos de cliente procesados:")
    print(f"  👤 Normales: {concurrent_stats.client_type_counts.get('NORMAL', 0)}")
    print(f"  ⭐ VIP:      {concurrent_stats.client_type_counts.get('VIP', 0)}")
    print(f"  🤖 Bots:     {concurrent_stats.client_type_counts.get('BOT', 0)}")
    print_ranking(products)
    print("-" * 72)
    print(f"Informe visual:       {report_path}")
    print("=" * 72)


# ============================================================
# PROGRAMA PRINCIPAL
# ============================================================


def parse_args() -> SimulationConfig:
    parser = argparse.ArgumentParser(
        description="Simulador concurrente de una tienda online durante Black Friday."
    )
    parser.add_argument("--clients", type=int, default=1200, help="Número de clientes/pedidos simulados.")
    parser.add_argument("--workers", type=int, default=12, help="Número de servidores que procesan pedidos.")
    parser.add_argument("--payment-slots", type=int, default=5, help="Pagos simultáneos permitidos.")
    parser.add_argument("--max-quantity", type=int, default=2, help="Cantidad máxima por pedido.")
    parser.add_argument("--unsafe", action="store_true", help="Ejecuta la versión concurrente sin Lock de stock.")
    parser.add_argument("--no-live", action="store_true", help="Desactiva el panel visual en directo.")
    parser.add_argument("--seed", type=int, default=7, help="Semilla aleatoria para resultados reproducibles.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_DIR, help="Carpeta donde guardar el informe visual.")
    args = parser.parse_args()

    if args.clients < 1:
        raise ValueError("El número de clientes debe ser mayor que 0.")
    if args.workers < 1:
        raise ValueError("El número de servidores debe ser mayor que 0.")
    if args.payment_slots < 1:
        raise ValueError("Debe haber al menos una pasarela de pago disponible.")
    if args.max_quantity < 1:
        raise ValueError("La cantidad máxima por pedido debe ser mayor que 0.")

    return SimulationConfig(
        clients=args.clients,
        workers=args.workers,
        payment_slots=args.payment_slots,
        max_quantity=args.max_quantity,
        unsafe=args.unsafe,
        seed=args.seed,
        live=not args.no_live,
        output_dir=args.output,
    )


def main() -> None:
    config = parse_args()

    if config.seed is not None:
        random.seed(config.seed)

    base_products = build_products()
    orders = build_orders(config, base_products)

    clear_screen()
    print(shop_banner())
    print_catalog(base_products)
    print("Arrancando simulación...\n")
    time.sleep(0.8 if config.live else 0.0)

    # Versión secuencial para comparación.
    sequential_products = clone_products(base_products)
    sequential_orders = [Order(**order.__dict__) for order in orders]
    sequential_stats, sequential_time = run_sequential_simulation(sequential_products, sequential_orders)

    # Versión concurrente principal.
    concurrent_products = clone_products(base_products)
    concurrent_orders = [Order(**order.__dict__) for order in orders]
    concurrent_stats, concurrent_time = run_concurrent_simulation(config, concurrent_products, concurrent_orders)

    # Mini prueba que explica por qué el Lock es importante.
    race_message = race_condition_demo()

    report_path = generate_report(
        output_dir=Path(config.output_dir),
        concurrent_stats=concurrent_stats,
        concurrent_time=concurrent_time,
        sequential_stats=sequential_stats,
        sequential_time=sequential_time,
        products=concurrent_products,
        config=config,
        race_message=race_message,
    )

    print_final_summary(
        concurrent_stats=concurrent_stats,
        concurrent_time=concurrent_time,
        sequential_stats=sequential_stats,
        sequential_time=sequential_time,
        report_path=report_path,
        config=config,
        products=concurrent_products,
    )


if __name__ == "__main__":
    main()
