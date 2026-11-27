#pragma once

#include "api/masterclient.h"
#include <QDateTime>
#include <QMap>
#include <QWidget>

class QComboBox;
class QLabel;
class QTimer;
class PhysicalPlot;

class NodeView final : public QWidget {
    Q_OBJECT
public:
    explicit NodeView(QWidget *parent = nullptr);
    void configure(const QUrl &url);
    void selectNode(const QString &node);
    QString node() const { return node_; }
    int interval() const;
    void setInterval(int index);
    void refresh(const QStringList &hosts, const QDateTime &minimum, const QDateTime &at, const QString &execution);
signals:
    void contextChanged();
private:
    struct Request {
        QString node;
        QString execution;
        QDateTime from, to;
        bool operator==(const Request &) const = default;
    };
    void request();
    void finish(const Request &request, quint64 generation, const HttpResult &result);
    void retry();
    void display();
    void setCursor(int index);

    QComboBox *nodes_;
    QComboBox *interval_;
    QLabel *title_;
    QLabel *moment_;
    QLabel *status_;
    QLabel *cursorText_;
    QList<QLabel *> values_;
    QList<PhysicalPlot *> plots_;
    MasterClient client_;
    QTimer *delay_;
    QUrl archiveUrl_;
    QString node_, source_, profile_, sampleScope_;
    Request desired_;
    bool busy_ = false;
    quint64 generation_ = 0;
    QMap<QDateTime, QList<double>> samples_;
    int cursor_ = -1;
};
