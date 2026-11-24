#include "reviewrun.h"
#include "models/monitorstore.h"

#include <QDir>
#include <QFile>
#include <QLocale>

namespace {
QJsonObject readObject(const QString &path)
{
    QFile file(path);
    return file.open(QIODevice::ReadOnly) ? QJsonDocument::fromJson(file.readAll()).object() : QJsonObject{};
}

QString formattedCount(const QJsonValue &number)
{
    return number.isDouble() ? QLocale(QLocale::Russian).toString(number.toInteger()) : QStringLiteral("—");
}
}

QString ReviewRun::period() const
{
    if (!start.isValid() || !end.isValid()) return {};
    if (start.date() == end.date())
        return QStringLiteral("%1 · %2–%3 UTC").arg(start.toString("dd.MM.yyyy"), start.toString("HH:mm"), end.toString("HH:mm"));
    return timeText(start) + QStringLiteral(" — ") + timeText(end);
}

QList<ReviewRun> readReviewRuns(const QString &root, int pollInterval)
{
    QList<ReviewRun> runs;
    if (root.isEmpty()) return runs;
    for (const auto &name : {QString("d2-max"), QString("smoke-live")}) {
        const QDir dir(QDir(root).filePath(name));
        const auto data = readObject(dir.filePath("demo.json"));
        const auto report = readObject(dir.filePath("verification.json"));
        ReviewRun run;
        run.connection = {QUrl(data.value("master").toString()), data.value("execution").toString(), pollInterval};
        if (!MasterClient::validUrl(run.connection.masterUrl) || !MasterClient::validExecution(run.connection.execution))
            continue;
        run.speed = data.value("speed").toString();
        run.name = run.online() ? QStringLiteral("Summit — проверка онлайн-мониторинга") : QStringLiteral("Summit D2 — полный анализ");
        run.shortName = run.online() ? QStringLiteral("Summit · LIVE тест") : QStringLiteral("Summit D2 · MAX");
        run.start = readTime(data.value("start"));
        run.end = readTime(data.value("end"));
        run.events = formattedCount(report.value("terminalEvents"));
        run.ticks = formattedCount(report.value("groupTicks"));
        run.verified = report.value("summary").toObject().value("complete").toBool()
            && report.value("summary").toObject().value("execution_id").toString() == run.connection.execution;
        auto seconds = readObject(dir.filePath("durable-timing.json")).value("secondsFromReplayStartToFinalAck");
        if (!seconds.isDouble()) seconds = report.value("pipeline").toObject().value("wallSeconds");
        if (seconds.isDouble()) {
            const auto total = seconds.toDouble();
            run.duration = QStringLiteral("%1 мин %2 с").arg(int(total / 60))
                .arg(QLocale(QLocale::Russian).toString(total - 60 * int(total / 60), 'f', 1));
        }
        runs << run;
    }
    return runs;
}
