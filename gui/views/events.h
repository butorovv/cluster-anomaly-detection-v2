#pragma once

#include "models/eventtable.h"
#include <QWidget>

class QComboBox;
class QLabel;
class QPlainTextEdit;
class QPushButton;
class QTableView;

class Events final : public QWidget {
    Q_OBJECT
public:
    explicit Events(QWidget *parent = nullptr);
    void refresh(const MonitorStore &store, const QDateTime &from, const QDateTime &to);
    void clear();

signals:
    void filterChanged();
    void eventSelected(const QString &group, const QDateTime &detected, const QDateTime &closed);

private:
    void showDetails();
    QComboBox *group_;
    QTableView *table_;
    QLabel *count_;
    QPlainTextEdit *details_;
    QPushButton *open_;
    EventTable *model_;
    qsizetype previousCount_ = -1;
    QString previousGroup_;
    QDateTime previousFrom_;
    QDateTime previousTo_;
};
