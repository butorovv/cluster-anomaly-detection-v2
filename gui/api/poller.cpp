#include "poller.h"
#include <QDir>
#include <QFile>

namespace {
const QString summaryPath = QStringLiteral("/v1/summary");
const QString metricsPath = QStringLiteral("/metrics");
const QString resultsPath = QStringLiteral("/v1/results");
const QString eventsPath = QStringLiteral("/v1/events");
constexpr int pageBudget = 48;

QJsonDocument readPage(const QString &path, qint64 maximum)
{
    QFile file(path);
    if (!file.open(QIODevice::ReadOnly) || file.size() > maximum) return {};
    return QJsonDocument::fromJson(file.readAll());
}
}

Poller::Poller(QObject *parent) : QObject(parent)
{
    connect(&timer_, &QTimer::timeout, this, &Poller::poll);
}

void Poller::configure(const ConnectionSettings &settings)
{
    if (generation_ > 0 && snapshot_.isEmpty() && settings_.masterUrl == settings.masterUrl && settings_.execution == settings.execution) {
        settings_ = settings;
        timer_.setInterval(settings_.intervalMs);
        return;
    }
    ++generation_;
    client_.cancel();
    snapshot_.clear();
    settings_ = settings;
    store_ = {};
    errors_.clear();
    pending_.clear();
    health_ = readiness_ = -1;
    resultHead_ = resultRepair_ = {};
    eventHead_.clear();
    eventRepair_.clear();
    repairingResults_ = repairingEvents_ = resultsBusy_ = eventsBusy_ = false;
    targetResults_ = targetEvents_ = 0;
    emit reset();
    timer_.start(settings_.intervalMs);
    poll();
}

void Poller::openSnapshot(const ConnectionSettings &settings, const QString &directory)
{
    if (snapshot_ == directory && settings_.execution == settings.execution) return;
    const auto generation = ++generation_;
    client_.cancel();
    timer_.stop();
    snapshot_ = directory;
    settings_ = settings;
    store_ = {};
    errors_.clear();
    health_ = readiness_ = -1;
    pending_.clear();
    targetResults_ = targetEvents_ = 0;
    emit reset();
    const QDir run(directory);
    const auto manifest = readPage(run.filePath("verification.json"), 1024 * 1024).object();
    const auto summary = manifest.value("summary").toObject();
    const auto pages = manifest.value("snapshot").toObject();
    const int expectedResults = pages.value("resultPages").toInt(-1);
    const int expectedEvents = pages.value("eventPages").toInt(-1);
    if (!summary.value("complete").toBool() || summary.value("execution_id").toString() != settings.execution ||
        expectedResults <= 0 || expectedResults > 360 || expectedEvents < 0 || expectedEvents > 461) {
        setError(summaryPath, tr("Архив запуска отсутствует или не прошёл проверку."));
        return;
    }
    targetResults_ = summary.value("group_ticks").toInteger();
    targetEvents_ = summary.value("parent_events").toInteger();
    store_.summary = summary;
    const QDir data(run.filePath("snapshot"));
    // A single page per event-loop dispatch gives paint and input events time to run.
    auto *loader = new QTimer(this);
    connect(loader, &QTimer::timeout, this,
        [this, loader, generation, data, expectedResults, expectedEvents,
         page = 0, resultAfter = ResultCursor{}, eventAfter = QString{}]() mutable {
        if (generation != generation_) { loader->deleteLater(); return; }
        if (page < expectedResults) {
            const auto json = readPage(data.filePath(QString("results-%1.json").arg(page)), 24 * 1024 * 1024);
            const auto result = ResultPage::parse(json, resultAfter);
            if (!result || result->items.isEmpty()) {
                loader->stop();
                loader->deleteLater();
                setError(resultsPath, tr("Не читается сохранённая страница результатов."));
                return;
            }
            resultAfter = result->next;
            store_.insert(result->items);
        } else if (page < expectedResults + expectedEvents) {
            const auto json = readPage(data.filePath(QString("events-%1.json").arg(page - expectedResults)), 24 * 1024 * 1024);
            const auto events = AnomalyEvent::parse(json, eventAfter);
            if (!events || events->isEmpty()) {
                loader->stop();
                loader->deleteLater();
                setError(eventsPath, tr("Не читается сохранённая страница событий."));
                return;
            }
            eventAfter = events->last().id;
            store_.insert(*events);
        } else {
            loader->stop();
            loader->deleteLater();
            if (!synchronized()) setError(summaryPath, tr("Число записей не совпало с подтверждённой сводкой."));
            emit updated();
            return;
        }
        if (++page % 8 == 0) emit updated();
    });
    loader->start(1);
}

bool Poller::stale() const
{
    return health_ == 0 || readiness_ == 0 || errors_.contains(summaryPath)
        || errors_.contains(resultsPath) || errors_.contains(eventsPath);
}

bool Poller::synchronized() const
{
    return !stale() && !store_.summary.isEmpty() && store_.resultCount() == targetResults_
        && store_.events().size() == targetEvents_;
}

void Poller::poll()
{
    probe(QStringLiteral("/healthz"), health_);
    probe(QStringLiteral("/readyz"), readiness_);
    fetchMetrics();
    if (!MasterClient::validExecution(settings_.execution)) return;
    fetchSummary();
    // Keep the live streams moving when the aggregate summary is slow or unavailable.
    // A subsequent summary still supplies the counts used to find earlier gaps.
    if (!store_.summary.value("complete").toBool() || errors_.contains(summaryPath)) {
        if (!resultsBusy_) fetchResults(pageBudget);
        if (!eventsBusy_) fetchEvents(pageBudget);
    }
}

void Poller::setError(const QString &path, const QString &message)
{
    if (message.isEmpty()) errors_.remove(path);
    else errors_.insert(path, message);
    emit updated();
}

void Poller::probe(const QString &path, int &state)
{
    if (pending_.contains(path)) return;
    pending_.insert(path);
    client_.get(settings_.masterUrl, path, {}, false, [this, generation = generation_, path, &state](HttpResult result) {
        if (generation != generation_) return;
        pending_.remove(path);
        state = result.ok() ? 1 : 0;
        setError(path, result.error);
    });
}

void Poller::fetchMetrics()
{
    if (pending_.contains(metricsPath)) return;
    pending_.insert(metricsPath);
    client_.get(settings_.masterUrl, metricsPath, {}, true, [this, generation = generation_](HttpResult result) {
        if (generation != generation_) return;
        pending_.remove(metricsPath);
        store_.metrics = result.ok() ? result.json.object() : QJsonObject{};
        setError(metricsPath, result.ok() && !result.json.isObject()
                              ? tr("Ожидался объект метрик") : result.error);
    });
}

void Poller::fetchSummary()
{
    if (pending_.contains(summaryPath)) return;
    pending_.insert(summaryPath);
    QUrlQuery query;
    query.addQueryItem("execution_id", settings_.execution);
    client_.get(settings_.masterUrl, summaryPath, query, true, [this, generation = generation_](HttpResult result) {
        if (generation != generation_) return;
        pending_.remove(summaryPath);
        const auto summary = result.json.object();
        if (!result.ok() || summary.value("execution_id").toString() != settings_.execution
            || !summary.value("group_ticks").isDouble() || !summary.value("parent_events").isDouble()) {
            setError(summaryPath, result.ok() ? tr("Некорректная сводка запуска") : result.error);
            return;
        }
        store_.summary = summary;
        targetResults_ = summary.value("group_ticks").toInteger();
        targetEvents_ = summary.value("parent_events").toInteger();
        setError(summaryPath, {});
        if (!resultsBusy_ && (repairingResults_ || store_.resultCount() != targetResults_ || errors_.contains(resultsPath)))
            fetchResults(pageBudget);
        if (!eventsBusy_ && (repairingEvents_ || store_.events().size() != targetEvents_ || errors_.contains(eventsPath)))
            fetchEvents(pageBudget);
    });
}

bool Poller::prepareRepair(Feed feed)
{
    if (feed == Feed::Results) {
        if (repairingResults_ || store_.resultCount() >= targetResults_) return false;
        repairingResults_ = true;
        resultRepair_ = store_.confirmedCursor();
        return true;
    }
    if (repairingEvents_ || store_.events().size() >= targetEvents_) return false;
    repairingEvents_ = true;
    eventRepair_.clear();
    return true;
}

void Poller::fetchResults(int budget)
{
    resultsBusy_ = true;
    const auto cursor = repairingResults_ ? resultRepair_ : resultHead_;
    QUrlQuery query;
    query.addQueryItem("execution_id", settings_.execution);
    query.addQueryItem("limit", "64");
    if (!cursor.tick.isEmpty()) {
        query.addQueryItem("after_tick", cursor.tick);
        query.addQueryItem("after_group", cursor.group);
    }
    client_.get(settings_.masterUrl, resultsPath, query, true,
                [this, generation = generation_, cursor, budget](HttpResult result) {
        if (generation != generation_) return;
        resultsBusy_ = false;
        const auto page = result.ok() ? ResultPage::parse(result.json, cursor) : std::nullopt;
        if (!page) {
            setError(resultsPath, result.ok() ? tr("Некорректная страница результатов") : result.error);
            return;
        }
        if (page->items.isEmpty()) {
            if (prepareRepair(Feed::Results)) {
                if (budget > 1) fetchResults(budget - 1);
            } else {
                repairingResults_ = false;
            }
            setError(resultsPath, {});
            return;
        }
        store_.insert(page->items);
        if (repairingResults_) resultRepair_ = page->next;
        const auto nextTime = readTime(page->next.tick);
        const auto headTime = readTime(resultHead_.tick);
        if (!headTime.isValid() || nextTime > headTime || (nextTime == headTime && page->next.group > resultHead_.group))
            resultHead_ = page->next;
        setError(resultsPath, {});
        if (budget > 1) fetchResults(budget - 1);
    });
}

void Poller::fetchEvents(int budget)
{
    eventsBusy_ = true;
    const auto cursor = repairingEvents_ ? eventRepair_ : eventHead_;
    QUrlQuery query;
    query.addQueryItem("execution_id", settings_.execution);
    query.addQueryItem("limit", "100");
    if (!cursor.isEmpty()) query.addQueryItem("after", cursor);
    client_.get(settings_.masterUrl, eventsPath, query, true,
                [this, generation = generation_, cursor, budget](HttpResult result) {
        if (generation != generation_) return;
        eventsBusy_ = false;
        const auto page = result.ok() ? AnomalyEvent::parse(result.json, cursor) : std::nullopt;
        if (!page) {
            setError(eventsPath, result.ok() ? tr("Некорректная страница событий") : result.error);
            return;
        }
        if (page->isEmpty()) {
            if (prepareRepair(Feed::Events)) {
                if (budget > 1) fetchEvents(budget - 1);
            } else {
                repairingEvents_ = false;
            }
            setError(eventsPath, {});
            return;
        }
        store_.insert(*page);
        const auto next = page->last().id;
        if (repairingEvents_) eventRepair_ = next;
        if (next > eventHead_) eventHead_ = next;
        setError(eventsPath, {});
        if (budget > 1) fetchEvents(budget - 1);
    });
}
