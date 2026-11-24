#include "reviewrun.h"
#include "models/monitorstore.h"

#include <QDir>
#include <QFile>
#include <QLocale>
#include <QTimeZone>
#include <algorithm>

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

QString durationText(double seconds)
{
    return QStringLiteral("%1 мин %2 с").arg(int(seconds / 60))
        .arg(QLocale(QLocale::Russian).toString(seconds - 60 * int(seconds / 60), 'f', 1));
}
}

QDateTime summitStart() { return {QDate(2022, 1, 18), QTime(8, 0), QTimeZone::utc()}; }
QDateTime summitEnd() { return summitStart().addSecs(8 * 3600); }

bool ReviewRun::active() const
{
    return phase == "preparing" || phase == "processing" || phase == "draining"
        || phase == "synchronizing" || phase == "stopping";
}

QString ReviewRun::status() const
{
    if (phase == "error") return QStringLiteral("Ошибка запуска");
    if (phase == "preparing") return QStringLiteral("Подготовка окружения");
    if (phase == "processing") return QStringLiteral("Обработка телеметрии");
    if (phase == "draining") return QStringLiteral("Финализация и доставка");
    if (phase == "synchronizing") return QStringLiteral("Сверка результатов");
    if (phase == "stopping") return QStringLiteral("Результаты сохранены · остановка окружения");
    return verified ? QStringLiteral("Завершён") : QStringLiteral("Требуется сверка");
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
        run.summitSource = true;
        run.onlineSimulation = name == "smoke-live";
        run.name = run.online() ? QStringLiteral("Summit — проверка онлайн-мониторинга") : QStringLiteral("Summit D2 — полный анализ");
        run.shortName = run.online() ? QStringLiteral("Summit · LIVE тест") : QStringLiteral("Summit D2 · MAX");
        run.start = readTime(data.value("start"));
        run.end = readTime(data.value("end"));
        run.created = readTime(readObject(dir.filePath("replay-start.json")).value("startedUtc"));
        run.events = formattedCount(report.value("terminalEvents"));
        run.ticks = formattedCount(report.value("groupTicks"));
        run.verified = report.value("summary").toObject().value("complete").toBool()
            && report.value("summary").toObject().value("execution_id").toString() == run.connection.execution;
        const QDir saved(QDir(root).filePath("v32-review-snapshots/" + name));
        const auto snapshotReport = readObject(saved.filePath("verification.json"));
        if (run.verified && snapshotReport.value("summary").toObject().value("execution_id").toString()
            == run.connection.execution && snapshotReport.value("groupTicks") == report.value("groupTicks")
            && snapshotReport.value("terminalEvents") == report.value("terminalEvents")
            && snapshotReport.value("snapshot").toObject().value("resultPages").toInt() > 0) {
            run.snapshot = true;
            run.directory = saved.absolutePath();
        }
        auto seconds = readObject(dir.filePath("durable-timing.json")).value("secondsFromReplayStartToFinalAck");
        if (!seconds.isDouble()) seconds = report.value("pipeline").toObject().value("wallSeconds");
        if (seconds.isDouble()) {
            run.duration = durationText(seconds.toDouble());
        }
        runs << run;
    }
    return runs;
}

QList<ReviewRun> readManagedRuns(const QString &root, int port, int pollInterval)
{
    QList<ReviewRun> runs;
    if (root.isEmpty()) return runs;
    const QDir base(root);
    for (const auto &entry : base.entryList(QDir::Dirs | QDir::NoDotAndDotDot | QDir::NoSymLinks)) {
        if (!MasterClient::validExecution(entry)) continue;
        const QDir folder(base.filePath(entry));
        const auto job = readObject(folder.filePath("request.json"));
        if (job.value("id").toString() != entry) continue;
        const auto state = readObject(folder.filePath("state.json"));
        const QDir environment(folder.filePath("environment"));
        const auto report = readObject(environment.filePath("verification.json"));
        const auto summary = report.value("summary").toObject();
        ReviewRun run;
        run.connection = {QUrl(QString("http://127.0.0.1:%1").arg(port)), entry, pollInterval};
        run.name = job.value("name").toString();
        run.start = readTime(job.value("start"));
        run.end = readTime(job.value("end"));
        run.created = readTime(job.value("created"));
        run.onlineSimulation = job.value("kind").toString() == "online";
        run.summitSource = true;
        run.speed = job.value("speed").toString();
        run.directory = environment.absolutePath();
        run.phase = state.value("phase").toString("preparing");
        run.phaseMessage = state.value("message").toString();
        run.snapshot = summary.value("complete").toBool() && summary.value("execution_id").toString() == entry
            && report.value("snapshot").toObject().value("resultPages").toInt() > 0;
        run.verified = run.snapshot && run.phase == "complete";
        if (run.phase == "processing") {
            if (environment.exists("pipeline.json")) run.phase = "synchronizing";
            else if (environment.exists("replay.json")) run.phase = "draining";
        }
        if (run.active() && state.value("pid").toInteger() > 0) {
#ifdef Q_OS_LINUX
            QFile command(QString("/proc/%1/cmdline").arg(state.value("pid").toInteger()));
            const bool owned = command.open(QIODevice::ReadOnly) && command.read(8192).split('\0').contains(entry.toUtf8());
            if (!owned) {
                run.phase = "error";
                run.phaseMessage = QStringLiteral("Процесс запуска завершился без подтверждения.");
            }
#endif
        } else if (state.isEmpty() && run.created.isValid() && run.created.secsTo(QDateTime::currentDateTimeUtc()) > 20) {
            run.phase = "error";
            run.phaseMessage = QStringLiteral("Процесс запуска не начал подготовку.");
        }
        if (run.snapshot) {
            run.events = formattedCount(report.value("terminalEvents"));
            run.ticks = formattedCount(report.value("groupTicks"));
            const auto duration = report.value("pipeline").toObject().value("wallSeconds");
            if (duration.isDouble()) run.duration = durationText(duration.toDouble());
        }
        runs << run;
    }
    std::sort(runs.begin(), runs.end(), [](const ReviewRun &a, const ReviewRun &b) { return a.created > b.created; });
    return runs;
}
