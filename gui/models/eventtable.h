#pragma once

#include "monitorstore.h"
#include <QAbstractTableModel>

class EventTable final : public QAbstractTableModel {
public:
    using QAbstractTableModel::QAbstractTableModel;
    void replace(const QMap<QString, AnomalyEvent> &events, const QString &group,
                 const QDateTime &from, const QDateTime &to);
    int rowCount(const QModelIndex &parent = {}) const override;
    int columnCount(const QModelIndex &parent = {}) const override;
    QVariant data(const QModelIndex &index, int role) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role) const override;
    const AnomalyEvent *at(int row) const;
    int rowOf(const QString &id) const;
private:
    QList<AnomalyEvent> rows_;
};
