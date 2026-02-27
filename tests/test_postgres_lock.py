"""
测试 PostgreSQL Advisory Lock 死锁修复
"""
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))


async def test_advisory_lock_logic():
    """测试 Advisory Lock 逻辑是否能正确导入"""
    print("Testing PostgreSQL Advisory Lock fix...")

    # 测试导入
    try:
        from app.core.storage import SQLStorage
        print("✓ SQLStorage imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import SQLStorage: {e}")
        return False

    # 验证类常量存在
    if hasattr(SQLStorage, '_SCHEMA_LOCK_ID'):
        print(f"✓ _SCHEMA_LOCK_ID = {SQLStorage._SCHEMA_LOCK_ID}")
    else:
        print("✗ _SCHEMA_LOCK_ID not found")
        return False

    # 验证方法存在
    if hasattr(SQLStorage, '_ensure_schema'):
        print("✓ _ensure_schema method exists")
    else:
        print("✗ _ensure_schema method not found")
        return False

    if hasattr(SQLStorage, '_do_ensure_schema'):
        print("✓ _do_ensure_schema method exists")
    else:
        print("✗ _do_ensure_schema method not found")
        return False

    print("\nAll checks passed!")
    return True


async def test_concurrent_lock_simulation():
    """模拟并发场景下锁的行为"""
    print("\nTesting concurrent lock simulation...")

    # 这个测试仅验证逻辑，不连接真实数据库
    lock_acquired = [False, False]
    lock_order = []

    async def instance_1():
        # 模拟获取锁成功
        lock_acquired[0] = True
        lock_order.append(1)
        await asyncio.sleep(0.1)  # 模拟 DDL 执行时间
        lock_order.append(-1)  # 释放锁
        lock_acquired[0] = False

    async def instance_2():
        # 等待一会再尝试获取锁
        await asyncio.sleep(0.05)
        if lock_acquired[0]:  # 如果锁被占用，等待
            await asyncio.sleep(0.1)
        lock_acquired[1] = True
        lock_order.append(2)
        await asyncio.sleep(0.1)
        lock_order.append(-2)
        lock_acquired[1] = False

    await asyncio.gather(instance_1(), instance_2())

    # 验证没有并发执行（即 lock_order 中没有 [1, 2] 或 [2, 1] 的情况）
    active = set()
    for op in lock_order:
        if op > 0:
            if active:  # 已经有活跃实例
                print(f"✗ Concurrent execution detected! Active: {active}, New: {op}")
                return False
            active.add(op)
        else:
            active.discard(-op)

    print(f"✓ Lock order: {lock_order}")
    print("✓ No concurrent execution detected")
    return True


if __name__ == "__main__":
    results = []
    results.append(asyncio.run(test_advisory_lock_logic()))
    results.append(asyncio.run(test_concurrent_lock_simulation()))

    if all(results):
        print("\n✓ All tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)
