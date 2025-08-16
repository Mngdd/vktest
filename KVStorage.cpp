#include <string>
#include <span>
#include <cstdint>
#include <utility>
#include <vector>
#include <optional>
#include <set>
#include <map>
#include <functional>
#include <limits>
#include <iostream>

template<typename Clock>
class KVStorage {
public:
    // Инициализирует хранилище переданным множеством записей. Размер span может быть очень большим.
    // Также принимает абстракцию часов (Clock) для возможности управления временем в тестах.
    explicit KVStorage(std::span<std::tuple<std::string /*key*/, std::string /*value*/, uint32_t /*ttl*/> > entries,
                       Clock clock = Clock()) : clock_(clock) {
        for (auto [key, value, ttl]: entries) {
            set(key, value, ttl);
        }
    }

    ~KVStorage() = default;

    // Присваивает по ключу key значение value.
    // Если ttl == 0, то время жизни записи - бесконечность, иначе запись должна перестать быть доступной через ttl секунд.
    // Безусловно обновляет ttl записи.
    // ------ сложность: logn
    void set(const std::string &key, const std::string &value, uint32_t ttl) {
        // при ОБНОВЛЕНИИ надо удалить старые данные из сета
        if (mapContains(key)) {
            tryToRemoveFromSet(key);
        }

        // при необходимости добавляем время
        uint64_t dt = getDeathTime_(ttl);
        if (ttl != 0) {
            expiration_set_.emplace(key, dt);
        }

        kv_map_[key] = timedKVMember{value, dt};

        std::cout << "------" << std::endl;
        std::cout << kv_map_.size() << " " << sizeof(kv_map_) << std::endl;
        std::cout << sizeof(timedKVMember{value, dt}) << " " << sizeof(value) << " " << sizeof(int) << std::endl;
    }

    // Удаляет запись по ключу key.
    // Возвращает true, если запись была удалена. Если ключа не было до удаления, то вернет false.
    // ------ сложность: logn
    bool remove(std::string_view key) {
        std::string skey = std::string(key);
        // как я понял можно удалять и протухшие, так что просто проверка на ключ делается
        if (!mapContains(skey))
            return false;
        tryToRemoveFromSet(skey);
        kv_map_.erase(skey);

        return true;
    }

    // Получает значение по ключу key. Если данного ключа нет, то вернет std::nullopt.
    // МОЖНО ПОЛУЧИТЬ ТОЛЬКО НЕ ПРОТУХШИЕ ЗАПИСИ (у которых death_time > now)
    // ------ сложность: logn
    std::optional<std::string> get(std::string_view key) {
        if (!keyAvailable(key)) {
            return std::nullopt;
        }
        return std::make_optional(kv_map_[std::string(key)].value);
    }

    // Возвращает следующие count записей начиная с key в порядке лексикографической сортировки ключей.
    // Пример: ("a", "val1"), ("b", "val2"), ("d", "val3"), ("e", "val4")
    // getManySorted("c", 2) -> ("d", "val3"), ("e", "val4")
    // ------ сложность: n
    std::vector<std::pair<std::string, std::string> > getManySorted(std::string_view key, uint32_t count)  {
        if (count == 0)
            return {};
        std::vector<std::pair<std::string, std::string> > result{};

        auto now = static_cast<uint64_t>(clock_());
        for (auto it = kv_map_.begin(); it != kv_map_.end() && count > 0; ++it) {
            if (it->second.death_time <= now)
                continue;

            if (it->first >= std::string(key)) {
                result.emplace_back(it->first, it->second.value);
                --count;
            }
        }

        return result;
    }

    // Удаляет протухшую запись из структуры и возвращает ее. Если удалять нечего, то вернет std::nullopt.
    // Если на момент вызова метода протухло несколько записей, то можно удалить любую.
    // ------ сложность: logn
    std::optional<std::pair<std::string, std::string> > removeOneExpiredEntry() {
        auto now = static_cast<uint64_t>(clock_());

        if (expiration_set_.empty() || expiration_set_.begin()->death_time > now)
            return std::nullopt;
        auto key = expiration_set_.begin()->map_key;
        auto removed = std::pair<std::string, std::string>{key, kv_map_[key].value};

        remove(key);

        return std::make_optional(removed);
    }

private:
    // возвращает время смерти с учетом ttl относительно текущего момента
    // ------ сложность: const
    uint64_t getDeathTime_(uint32_t ttl) const {
        return (ttl == 0) ? maxTime_ : static_cast<uint64_t>(ttl) + static_cast<uint64_t>(clock_());
    }

    struct timedSetMember {
        std::string map_key;
        uint64_t death_time{};
    };

    struct timedKVMember {
        std::string value;
        uint64_t death_time{};
    };

    // основное хранилище, less<> ибо мы сравниваем иногда string со string_view
    std::map<std::string, timedKVMember, std::less<> > kv_map_;

    // храним в порядке возрастания времени смерти значения
    // std::function<bool(const timedSetMember &, const timedSetMember &)>
    // cmp_ = [](const timedSetMember &lhs, const timedSetMember &rhs) { return lhs.death_time < rhs.death_time; };
    struct timedSetComparator {
        bool operator()(const timedSetMember &lhs, const timedSetMember &rhs) const {
            return lhs.death_time < rhs.death_time
            || (lhs.death_time == rhs.death_time && lhs.map_key < rhs.map_key);
        }
    };
    std::set<timedSetMember, timedSetComparator> expiration_set_;

    // часы выбранные юзером
    Clock clock_;
    // в целом это время достижимо, и при сравнении death_time > now мы получим протухание...
    uint64_t maxTime_ = std::numeric_limits<uint64_t>::max();

    // удаляет связанное с данным key значение из сета expiration_set_
    // мы ЗАРАНЕЕ обязаны проверить что ключ СУЩЕСТВУЕТ, иначе бред!!!
    // ------ сложность: logn
    void tryToRemoveFromSet(const std::string &key) {
        // возможно до этого было ttl=0 -> этой записи в сете не будет
        auto tmp = timedSetMember{key, kv_map_[key].death_time};
        if (auto it = expiration_set_.find(tmp); it != expiration_set_.end())
            expiration_set_.erase(it);
    }

    // ------ сложность: logn
    bool mapContains(const std::string &key)  {
        return kv_map_.contains(key);
    }

    // может ли юзер получить данные по ключу? (не может если протухла запись или ключа нет)
    // ------ сложность: logn
    bool keyAvailable(std::string_view key) {
        std::string skey = std::string(key);
        auto now = static_cast<uint64_t>(clock_());
        // ключ существует вообще?
        if (!mapContains(skey))
            return false;

        timedSetMember tmp = timedSetMember{skey, kv_map_[skey].death_time};
        if (auto it = expiration_set_.find(tmp); it != expiration_set_.end()) {
            return it->death_time > now;
        }

        // ключ есть, в сете его нет значит ttl=0
        return true;
    }
};
