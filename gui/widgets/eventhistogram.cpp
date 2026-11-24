#include "eventhistogram.h"

#include <QHelpEvent>
#include <QKeyEvent>
#include <QMouseEvent>
#include <QPainter>
#include <QToolTip>
#include <algorithm>
#include <QtMath>

namespace { constexpr int inset = 16; }

EventHistogram::EventHistogram(QWidget *parent) : QWidget(parent)
{
    setFixedHeight(131);
    setFocusPolicy(Qt::StrongFocus);
    setAccessibleName(tr("Распределение событий по времени"));
}

void EventHistogram::setData(const QList<QDateTime> &starts, const QDateTime &minimum, const QDateTime &maximum,
                             const QDateTime &from, const QDateTime &to)
{
    minimum_ = minimum;
    maximum_ = maximum;
    from_ = from;
    to_ = to;
    counts_.clear();
    const auto span = minimum.secsTo(maximum);
    if (minimum.isValid() && maximum.isValid() && span > 0) {
        for (int candidate : {10, 30, 60, 300, 900, 1800, 3600, 7200, 21600, 86400}) {
            seconds_ = candidate;
            if (span <= qint64(candidate) * 8) break;
        }
        const auto epoch = minimum.toSecsSinceEpoch();
        base_ = minimum.addSecs(-(epoch % seconds_ + seconds_) % seconds_);
        const auto count = (base_.secsTo(maximum) + seconds_ - 1) / seconds_;
        if (count > 60) seconds_ *= int((count + 59) / 60);
        counts_.fill(0, (base_.secsTo(maximum) + seconds_ - 1) / seconds_);
        for (const auto &time : starts) {
            if (time < minimum || time >= maximum) continue;
            ++counts_[base_.secsTo(time) / seconds_];
        }
    }
    cursor_ = qMin(cursor_, int(counts_.size()) - 1);
    update();
}

int EventHistogram::binAt(qreal x) const
{
    if (x < inset || x >= width() - inset || counts_.isEmpty()) return -1;
    return int((x - inset) * counts_.size() / (width() - 2 * inset));
}

void EventHistogram::paintEvent(QPaintEvent *)
{
    QPainter p(this);
    p.setRenderHint(QPainter::Antialiasing);
    if (counts_.isEmpty()) {
        p.setPen(QColor("#697b91"));
        p.drawText(rect(), Qt::AlignCenter, tr("Нет данных за обработанный период"));
        return;
    }
    const int maximum = qMax(1, *std::max_element(counts_.cbegin(), counts_.cend()));
    const qreal step = qreal(width() - 2 * inset) / counts_.size();
    const int baseline = height() - 25;
    p.setPen(QColor("#dce3ec"));
    p.drawLine(inset, baseline, width() - inset, baseline);
    for (qsizetype i = 0; i < counts_.size(); ++i) {
        const auto lo = base_.addSecs(i * seconds_);
        const auto hi = lo.addSecs(seconds_);
        const bool active = from_.isValid() && from_ < hi && to_ > lo;
        const int size = qMax(2, qRound(84.0 * counts_[i] / maximum));
        const qreal x = inset + i * step;
        p.setPen(hasFocus() && cursor_ == i ? QColor("#192b40") : Qt::NoPen);
        p.setBrush(QColor(active ? "#295ec5" : from_.isValid() ? "#c4d5ef" : "#8daee1"));
        const qreal gap = qMin(14.0, step * .18);
        p.drawRoundedRect(QRectF(x + gap, baseline - size, step - 2 * gap, size), 4, 4);
        p.setPen(QColor("#697b91"));
        p.drawText(QRectF(x, baseline - size - 22, step, 20), Qt::AlignCenter, QString::number(counts_[i]));
        if (step >= 52 || i % qMax(1, qCeil(52 / step)) == 0)
            p.drawText(QRectF(x, baseline + 3, qMax(step, 52.0), 20), Qt::AlignCenter,
                       lo.toString(seconds_ < 60 ? "HH:mm:ss" : "HH:mm"));
    }
}

void EventHistogram::selectBin(int index)
{
    if (index < 0 || index >= counts_.size() || !counts_[index]) return;
    const auto start = base_.addSecs(index * seconds_);
    emit intervalSelected(qMax(start, minimum_), qMin(start.addSecs(seconds_), maximum_));
}

void EventHistogram::mouseReleaseEvent(QMouseEvent *event)
{
    if (event->button() == Qt::LeftButton) {
        cursor_ = binAt(event->position().x());
        selectBin(cursor_);
        update();
    }
}

void EventHistogram::keyPressEvent(QKeyEvent *event)
{
    if (counts_.isEmpty()) return;
    switch (event->key()) {
    case Qt::Key_Left: cursor_ = qMax(0, cursor_ - 1); break;
    case Qt::Key_Right: cursor_ = qMin(int(counts_.size()) - 1, cursor_ + 1); break;
    case Qt::Key_Return: case Qt::Key_Space: selectBin(cursor_); break;
    default: QWidget::keyPressEvent(event); return;
    }
    update();
}

bool EventHistogram::event(QEvent *event)
{
    if (event->type() == QEvent::ToolTip) {
        const auto *help = static_cast<QHelpEvent *>(event);
        const int index = binAt(help->pos().x());
        if (index >= 0) {
            const auto lo = base_.addSecs(index * seconds_);
            QToolTip::showText(help->globalPos(), tr("%1–%2 UTC · Событий: %3")
                .arg(lo.toString("dd.MM HH:mm:ss"), lo.addSecs(seconds_).toString("HH:mm:ss"))
                .arg(counts_[index]), this);
        }
        return true;
    }
    return QWidget::event(event);
}
