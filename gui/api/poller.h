#pragma once

#include "masterclient.h"
#include "models/monitorstore.h"

#include <QTimer>

class Poller final : public QObject {
    Q_OBJECT
public:
    explicit Poller(QObject *parent = nullptr);
    void configure(const ConnectionSettings &settings);
    const MonitorStore &store() const { return store_; }
    const ConnectionSettings &settings() const { return settings_; }
    const QMap<QString, QString> &errors() const { return errors_; }
    bool stale() const;
    bool synchronized() const;
    int health() const { return health_; }
    int readiness() const { return readiness_; }

signals:
    void updated();
    void reset();

private:
    enum class Feed { Results, Events };
    void poll();
    void probe(const QString &path, int &state);
    void fetchSummary();
    void fetchMetrics();
    void fetchResults(int budget);
    void fetchEvents(int budget);
    bool prepareRepair(Feed feed);
    void setError(const QString &path, const QString &message);

    MasterClient client_;
    QTimer timer_;
    ConnectionSettings settings_;
    MonitorStore store_;
    QMap<QString, QString> errors_;
    QSet<QString> pending_;
    quint64 generation_ = 0;
    int health_ = -1;
    int readiness_ = -1;
    ResultCursor resultHead_;
    ResultCursor resultRepair_;
    QString eventHead_;
    QString eventRepair_;
    bool repairingResults_ = false;
    bool repairingEvents_ = false;
    bool resultsBusy_ = false;
    bool eventsBusy_ = false;
    qint64 targetResults_ = 0;
    qint64 targetEvents_ = 0;
};
