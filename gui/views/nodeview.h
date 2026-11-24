#pragma once

#include <QDateTime>
#include <QMap>
#include <QWidget>

class QComboBox;
class QLabel;
class QProcess;
class QTimer;
class PhysicalPlot;

class NodeView final : public QWidget {
    Q_OBJECT
public:
    explicit NodeView(QWidget *parent = nullptr);
    void configure(const QString &assets);
    void selectNode(const QString &node);
    QString node() const { return node_; }
    int interval() const;
    void setInterval(int index);
    void refresh(const QStringList &hosts, const QDateTime &minimum, const QDateTime &at, bool sourceKnown);
signals:
    void contextChanged();
private:
    struct Request {
        QString node;
        QDateTime from, to;
        bool operator==(const Request &) const = default;
    };
    void request();
    void finish(bool success, const QByteArray &data);
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
    QProcess *process_;
    QTimer *delay_;
    QString assets_, node_;
    Request desired_, busy_, loaded_;
    QMap<QDateTime, QList<double>> samples_;
    int cursor_ = -1;
};
