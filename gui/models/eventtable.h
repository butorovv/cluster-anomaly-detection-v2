#pragma once

#include "monitorstore.h"
#include <QAbstractTableModel>

class EventTable final : public QAbstractTableModel {
public:
    using QAbstractTableModel::QAbstractTableModel;
    void replace(const QMap<QString, AnomalyEvent> &events, const QString &group,
                 const QDateTime &from, const QDateTime &to, const QString &criterion = {}, int limit = 0);
    static bool matches(const AnomalyEvent &event, const QString &group, const QString &criterion);
    static QString criteria(const AnomalyEvent &event);
    static QByteArray csv(const QList<AnomalyEvent> &events);
    int rowCount(const QModelIndex &parent = {}) const override;
    int columnCount(const QModelIndex &parent = {}) const override;
    QVariant data(const QModelIndex &index, int role) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role) const override;
    const AnomalyEvent *at(int row) const;
    int rowOf(const QString &id) const;
private:
    QList<AnomalyEvent> rows_;
};
