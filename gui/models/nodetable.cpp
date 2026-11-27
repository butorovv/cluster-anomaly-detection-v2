#include "nodetable.h"
#include <QColor>

namespace {
const QStringList headers{QStringLiteral("Узел"), QStringLiteral("Isolation Forest"), QStringLiteral("Оценка"),
                          QStringLiteral("Порог узла"), QStringLiteral("Статус диагностики")};
}

void NodeTable::setTick(const GroupTick *tick)
{
    QMap<QString, QJsonObject> nodes;
    if (tick) {
        for (const auto &value : tick->raw.value("local").toArray()) {
            const auto node = value.toObject();
            nodes.insert(node.value("hostname").toString(), node);
        }
        for (const auto &missing : tick->raw.value("missing_hosts").toArray()) {
            const auto host = missing.toString();
            if (!nodes.contains(host)) nodes.insert(host, QJsonObject{{"hostname", host}});
        }
    }
    const auto next = nodes.values();
    if (next == rows_) return;
    beginResetModel();
    rows_ = next;
    endResetModel();
}

int NodeTable::rowCount(const QModelIndex &parent) const { return parent.isValid() ? 0 : int(rows_.size()); }
int NodeTable::columnCount(const QModelIndex &parent) const { return parent.isValid() ? 0 : int(headers.size()); }

QVariant NodeTable::data(const QModelIndex &index, int role) const
{
    if (!index.isValid() || index.row() >= rows_.size()) return {};
    const auto &row = rows_.at(index.row());
    const auto decision = row.value("is_anomaly");
    const bool available = row.value("is_available").toBool() && decision.isBool();
    if (role == Qt::ForegroundRole && index.column() == 1)
        return QColor(available ? (decision.toBool() ? "#ad2743" : "#157149") : "#53657a");
    if (role == Qt::ToolTipRole) {
        const auto detail = row.value("diagnostic").toString();
        return detail.isEmpty() ? row.value("diagnostic_status").toString() : detail;
    }
    if (role != Qt::DisplayRole) return {};
    switch (index.column()) {
    case 0: return row.value("hostname").toString();
    case 1: return available ? stateText(decision.toBool() ? GroupState::Positive : GroupState::Negative)
                             : QStringLiteral("Нет результата");
    case 2: return numberText(row.value("anomaly_score"));
    case 3: return numberText(row.value("individual_threshold"));
    case 4: return row.value("diagnostic_status").toString(QStringLiteral("—"));
    default: return {};
    }
}

QVariant NodeTable::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (role == Qt::DisplayRole && orientation == Qt::Horizontal) return headers.value(section);
    return QAbstractTableModel::headerData(section, orientation, role);
}
