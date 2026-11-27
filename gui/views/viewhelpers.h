#pragma once

#include "models/monitorstore.h"

#include <QLabel>
#include <QProgressBar>

inline QLabel *sectionLabel(const QString &text)
{
    auto *label = new QLabel(text);
    label->setObjectName("section");
    return label;
}

inline QLabel *plainLabel(const QString &text = {})
{
    auto *label = new QLabel;
    label->setTextFormat(Qt::PlainText);
    label->setText(text);
    label->setWordWrap(true);
    return label;
}

inline QString stateColor(GroupState state)
{
    switch (state) {
    case GroupState::Negative: return "#16866b";
    case GroupState::Positive: return "#c43648";
    case GroupState::Unknown: return "#b97826";
    case GroupState::Missing: return "#8291a5";
    }
    return {};
}

inline QString durationText(qint64 seconds)
{
    return QStringLiteral("%1:%2:%3")
        .arg(seconds / 3600, 2, 10, QLatin1Char('0'))
        .arg(seconds / 60 % 60, 2, 10, QLatin1Char('0'))
        .arg(seconds % 60, 2, 10, QLatin1Char('0'));
}

inline QString receiptDuration(const QJsonObject &summary)
{
    const auto start = readTime(summary.value("first_result_at"));
    const auto end = readTime(summary.value("last_result_at"));
    return start.isValid() && end.isValid() && end >= start
         ? durationText(start.secsTo(end)) : QStringLiteral("—");
}

inline void setProgress(QProgressBar *bar, const QJsonObject &summary)
{
    const auto count = summary.value("group_ticks").toInteger();
    const auto expected = summary.value("expected_ticks").toInteger();
    bar->setRange(0, 1000);
    bar->setValue(expected > 0 ? qBound(0, int(count * 1000.0 / expected), 1000) : 0);
    bar->setFormat(expected > 0 ? QStringLiteral("%1 / %2").arg(count).arg(expected)
                                 : QStringLiteral("Прогресс недоступен"));
}
