#include "mainwindow.h"
#include "style.h"

#include <QApplication>
#include <QCommandLineParser>
#include <QMessageBox>
#include <QStyleFactory>

int main(int argc, char *argv[])
{
    QApplication app(argc, argv);
    app.setOrganizationName("ClusterDiagnostics");
    app.setApplicationName("ClusterMonitor");
    app.setStyle(QStyleFactory::create("Fusion"));
    app.setStyleSheet(v2::style());
    QCommandLineParser parser;
    parser.setApplicationDescription(QStringLiteral("Рабочее место оператора диагностики"));
    parser.addHelpOption();
    parser.addOptions({
        {{"m", "master"}, QStringLiteral("Адрес Master API"), "url"},
        {{"e", "execution"}, QStringLiteral("UUID запуска"), "uuid"},
        {"review-root", QStringLiteral("Метаданные сохранённых запусков для визуальной приёмки"), "path"},
        {"summit-speed", QStringLiteral("Заявленный темп исторического Summit Replayer (1, 5, 10, 0=MAX)"), "speed"}
    });
    parser.process(app);
    QSettings preferences;
    ConnectionSettings connection;
    connection.masterUrl = QUrl(parser.isSet("master") ? parser.value("master")
        : preferences.value("masterUrl", connection.masterUrl.toString()).toString());
    connection.execution = (parser.isSet("execution") ? parser.value("execution")
        : preferences.value("execution").toString()).trimmed().toLower();
    connection.intervalMs = qBound(250, preferences.value("intervalMs", 1000).toInt(), 60000);
    const auto speed = parser.value("summit-speed");
    const bool wrongSource = parser.isSet("summit-speed")
        && (!parser.isSet("execution") || !QStringList{"1", "5", "10", "0"}.contains(speed));
    if (!MasterClient::validUrl(connection.masterUrl)
        || (!connection.execution.isEmpty() && !MasterClient::validExecution(connection.execution)) || wrongSource) {
        QMessageBox::critical(nullptr, QStringLiteral("Настройки подключения"),
                              QStringLiteral("Проверьте адрес Master, Execution ID и скорость из конфигурации демонстрации."));
        return 2;
    }
    MainWindow window(connection, speed, readReviewRuns(parser.value("review-root"), connection.intervalMs));
    window.show();
    return app.exec();
}
