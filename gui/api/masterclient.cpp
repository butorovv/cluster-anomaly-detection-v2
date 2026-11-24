#include "masterclient.h"

#include <QNetworkReply>
#include <QNetworkProxy>
#include <QRegularExpression>

MasterClient::MasterClient(QObject *parent) : QObject(parent)
{
    manager_.setProxy(QNetworkProxy::NoProxy);
}

bool MasterClient::validUrl(const QUrl &url)
{
    return url.isValid() && !url.host().isEmpty()
        && (url.scheme() == "http" || url.scheme() == "https")
        && url.userInfo().isEmpty() && !url.hasQuery() && !url.hasFragment();
}

bool MasterClient::validExecution(const QString &execution)
{
    static const QRegularExpression uuid(
        QStringLiteral("\\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\z"));
    return uuid.match(execution).hasMatch();
}

void MasterClient::get(const QUrl &base, const QString &path, const QUrlQuery &query,
                       bool expectsJson, std::function<void(HttpResult)> done)
{
    QUrl url = base;
    QString prefix = base.path();
    while (prefix.endsWith('/'))
        prefix.chop(1);
    url.setPath(prefix + path);
    url.setQuery(query);
    QNetworkRequest request(url);
    request.setTransferTimeout(path == "/v1/summary" ? 15000 : 5000);
    request.setAttribute(QNetworkRequest::RedirectPolicyAttribute, QNetworkRequest::ManualRedirectPolicy);
    request.setRawHeader("Accept", expectsJson ? "application/json" : "*/*");
    auto *reply = manager_.get(request);
    connect(reply, &QNetworkReply::finished, this, [reply, expectsJson, done = std::move(done)] {
        HttpResult result{reply->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt(), {}, {}};
        if (result.status == 0)
            result.error = reply->errorString();
        else if (result.status != 200)
            result.error = tr("HTTP %1").arg(result.status);
        else if (reply->error() != QNetworkReply::NoError)
            result.error = reply->errorString();
        else if (expectsJson) {
            QJsonParseError parseError;
            result.json = QJsonDocument::fromJson(reply->readAll(), &parseError);
            if (parseError.error != QJsonParseError::NoError)
                result.error = tr("Некорректный ответ JSON");
        }
        reply->deleteLater();
        done(std::move(result));
    });
}

void MasterClient::cancel()
{
    for (auto *reply : manager_.findChildren<QNetworkReply *>())
        if (reply->isRunning())
            reply->abort();
}
