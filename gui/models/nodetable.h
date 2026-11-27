#pragma once

#include <QAbstractTableModel>
#include "monitorstore.h"

class NodeTable final : public QAbstractTableModel {
public:
    using QAbstractTableModel::QAbstractTableModel;
    void setTick(const GroupTick *tick);
    int rowCount(const QModelIndex &parent = {}) const override;
    int columnCount(const QModelIndex &parent = {}) const override;
    QVariant data(const QModelIndex &index, int role) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role) const override;
private:
    QList<QJsonObject> rows_;
};
