#pragma once

#include <QJsonDocument>
#include <QNetworkAccessManager>
#include <QUrl>
#include <QUrlQuery>
#include <functional>

struct ConnectionSettings {
    QUrl masterUrl = QUrl(QStringLiteral("http://127.0.0.1:8080"));
    QString execution;
    int intervalMs = 1000;
};

struct HttpResult {
    int status;
    QString error;
    QJsonDocument json;
    bool ok() const { return status == 200 && error.isEmpty(); }
};

class MasterClient : public QObject {
    Q_OBJECT

public:
    explicit MasterClient(QObject *parent = nullptr);
    static bool validUrl(const QUrl &url);
    static bool validExecution(const QString &execution);
    void get(const QUrl &base, const QString &path, const QUrlQuery &query,
             bool expectsJson, std::function<void(HttpResult)> done);
    void cancel();

private:
    QNetworkAccessManager manager_;
};
