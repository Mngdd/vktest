#include <gtest/gtest.h>
#include <tuple>
#include <string>
#include <vector>
#include <optional>
#include <limits>
#include "KVStorage.cpp"
#define GTEST_COUT std::cout << "[INFO " << __func__ << ":l" << __LINE__ << "] "

struct FakeTimeManager {
    uint64_t time = 0;
    void advance(uint64_t seconds) noexcept { time += seconds; }
    void set(uint64_t t) noexcept { time = t; }
    uint64_t get() const { return time; }
};

struct FakeClock {
    explicit FakeClock(FakeTimeManager *timePtr) : timePtr_(timePtr) {
    }

    uint64_t operator()() const noexcept { return timePtr_->get(); }
    void advance(uint64_t seconds) noexcept { timePtr_->advance(seconds); }
    void set(uint64_t t) noexcept { timePtr_->set(t); }

private:
    FakeTimeManager *timePtr_;
};

using Entry = std::tuple<std::string, std::string, uint32_t>;

TEST(KVStorageTest, SetGetRemove) {
    std::vector<Entry> entries = {
        {"a", "1", 0},
        {"b", "2", 0}
    };
    FakeTimeManager timeManager;
    FakeClock clock(&timeManager);
    KVStorage<FakeClock> store(entries, clock);

    // проверяем гет
    EXPECT_EQ(store.get("a").value(), "1");
    EXPECT_EQ(store.get("b").value(), "2");
    EXPECT_FALSE(store.get("c").has_value());

    // новые ключи сетим
    store.set("c", "3", 0);
    EXPECT_EQ(store.get("c").value(), "3");

    // перезапись
    store.set("a", "10", 0);
    EXPECT_EQ(store.get("a").value(), "10");

    // проверка ремува
    EXPECT_TRUE(store.remove("b"));
    EXPECT_FALSE(store.get("b").has_value());
    EXPECT_FALSE(store.remove("b"));
    // по этому же ключу еще раз
    store.set("b", "ebra", 0);
    EXPECT_EQ(store.get("b").value(), "ebra");
    EXPECT_TRUE(store.remove("b"));
    EXPECT_FALSE(store.get("b").has_value());
    EXPECT_FALSE(store.remove("b"));

    // и последняя проверка что наши бессмертные значения реально бессмертные
    // в kv_map_ время смерти при ttl=0 стоит на максимальном значении uint64
    // а что если время будет точно такое же?
    auto expired = store.removeOneExpiredEntry();
    clock.set(std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(expired, std::nullopt);
}

TEST(KVStorageTest, GetManySorted) {
    std::vector<Entry> entries = {
        {"a", "1", 0},
        {"b", "2", 0},
        {"d", "4", 0},
        {"e", "5", 0},
        {"x", "j9", 0}
    };
    FakeTimeManager timeManager;
    FakeClock clock(&timeManager);
    KVStorage<FakeClock> store(entries, clock);

    // ну просто 2 элемента после несуществующего ищем
    auto result = store.getManySorted("c", 2);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], (std::pair{"d", "4"}));
    EXPECT_EQ(result[1], (std::pair{"e", "5"}));

    // выход за границы
    result = store.getManySorted("e", 3);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], (std::pair{"e", "5"}));
    EXPECT_EQ(result[1], (std::pair{"x", "j9"}));

    // взять одного
    result = store.getManySorted("a", 1);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (std::pair{"a", "1"}));

    // никого не нашли
    result = store.getManySorted("z", 10);
    EXPECT_TRUE(result.empty());

    // пустой поиск
    result = store.getManySorted("a", 0);
    EXPECT_TRUE(result.empty());

}

TEST(KVStorageTest, TTL) {
    std::vector<Entry> entries = {
        {"a", "1", 5},  // откиснет в 5
        {"b", "2", 0}
    };
    FakeTimeManager timeManager;
    FakeClock clock(&timeManager);
    KVStorage<FakeClock> store(entries, clock);

    // оба живы
    EXPECT_TRUE(store.get("a").has_value());
    EXPECT_TRUE(store.get("b").has_value());

    // 5 протухло
    clock.advance(5);
    auto expired = store.removeOneExpiredEntry();
    ASSERT_TRUE(expired.has_value());
    EXPECT_EQ(expired->first, "a");
    EXPECT_FALSE(store.get("a").has_value());
    EXPECT_TRUE(store.get("b").has_value());

    // перезапись времени вечноживущего объекта на конечное, проверка его истечения
    store.set("b", "тагилла", 5);  // скиснет в 10
    clock.set(11);
    expired = store.removeOneExpiredEntry();
    ASSERT_TRUE(expired.has_value());
    EXPECT_EQ(expired->first, "b");
    EXPECT_FALSE(store.get("b").has_value());

    // а если ничего не осталось?
    expired = store.removeOneExpiredEntry();
    EXPECT_EQ(expired, std::nullopt);
}

TEST(KVStorageTest, TTLWithQueue) {
    std::vector<Entry> entries = {
        {"a", "1", 2},
        {"b", "2", 0},
        {"d", "4", 3},
        {"e", "5", 1},
        {"x", "j9", 2}
    };
    FakeTimeManager timeManager;
    FakeClock clock(&timeManager);
    KVStorage<FakeClock> store(entries, clock);

    auto v1 = std::pair{"d", "4"}, v2 = std::pair{"x", "j9"};

    clock.set(5);
    auto result = store.getManySorted("c", 3);
    EXPECT_TRUE(result.empty());

    clock.set(1);
    result = store.getManySorted("c", 3);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], v1);
    EXPECT_EQ(result[1], v2);

    clock.set(2);
    result = store.getManySorted("c", 3);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], v1);
}

// а что если время смерти одинаковое (и значение)
TEST(KVStorageTest, TTLComparatorTest) {
    std::vector<Entry> entries = {
        {"a", "gm", 2},
        {"b", "gm", 2},
        {"c", "gm", 2}
    };
    FakeTimeManager timeManager;
    FakeClock clock(&timeManager);
    KVStorage<FakeClock> store(entries, clock);

    auto result = store.getManySorted("a", 3);
    ASSERT_EQ(result.size(), 3);

    clock.set(2);
    result = store.getManySorted("a", 3);
    ASSERT_EQ(result.size(), 0);

    auto expired = store.removeOneExpiredEntry();
    ASSERT_TRUE(expired.has_value());
    EXPECT_EQ(expired->first, "a");
    EXPECT_FALSE(store.get("a").has_value());

    expired = store.removeOneExpiredEntry();
    ASSERT_TRUE(expired.has_value());
    EXPECT_EQ(expired->first, "b");
    EXPECT_FALSE(store.get("b").has_value());

    expired = store.removeOneExpiredEntry();
    ASSERT_TRUE(expired.has_value());
    EXPECT_EQ(expired->first, "c");
    EXPECT_FALSE(store.get("c").has_value());

    expired = store.removeOneExpiredEntry();
    EXPECT_EQ(expired, std::nullopt);
}