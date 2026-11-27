#include "scorechart.h"

#include <QMouseEvent>
#include <QPainter>
#include <QPainterPath>
#include <QTimeZone>
#include <algorithm>
#include <cmath>
#include <limits>

ScoreChart::ScoreChart(QWidget *parent) : QWidget(parent)
{
    setFixedHeight(208);
    setAccessibleName(tr("График показателей по времени телеметрии"));
}

void ScoreChart::setSeries(QList<ChartSeries> series, const QDateTime &from, const QDateTime &to,
                           const QDateTime &selected)
{
    series_ = std::move(series);
    from_ = from;
    to_ = to;
    selected_ = selected;
    update();
}

QRectF ScoreChart::plotRect() const
{
    const auto gutter = qMax(48.0, width() * 0.12);
    return QRectF(rect()).adjusted(gutter + 24, 30, -gutter, -27);
}

void ScoreChart::paintEvent(QPaintEvent *)
{
    QPainter painter(this);
    painter.setRenderHint(QPainter::Antialiasing);
    auto font = painter.font();
    font.setPixelSize(11);
    painter.setFont(font);
    double yMin = std::numeric_limits<double>::infinity();
    double yMax = -yMin;
    for (const auto &series : series_)
        for (const auto &point : series.points) {
            yMin = std::min(yMin, point.y());
            yMax = std::max(yMax, point.y());
        }
    const auto plot = plotRect();
    if (!std::isfinite(yMin) || !from_.isValid() || !to_.isValid() || plot.width() <= 0) {
        painter.setPen(QColor("#52647a"));
        painter.drawText(rect(), Qt::AlignCenter, tr("Нет значений на выбранном интервале"));
        return;
    }
    const double spare = yMin == yMax ? qMax(0.1, std::abs(yMin) * 0.1) : (yMax - yMin) * 0.06;
    yMin -= spare;
    yMax += spare;
    const auto start = from_.toMSecsSinceEpoch();
    const auto duration = from_.msecsTo(to_);
    const auto span = qMax<qint64>(10000, duration);
    const auto x = [plot, start, span](double ms) { return plot.left() + (ms - start) * plot.width() / span; };
    const auto y = [plot, yMin, yMax](double value) {
        return plot.bottom() - (value - yMin) * plot.height() / (yMax - yMin);
    };
    for (int i = 0; i <= 4; ++i) {
        const double level = plot.bottom() - i * plot.height() / 4;
        painter.setPen(QColor("#e6ebf1"));
        painter.drawLine(QPointF(plot.left(), level), QPointF(plot.right(), level));
        painter.setPen(QColor("#52647a"));
        painter.drawText(QRectF(plot.left() - 66, level - 10, 59, 20), Qt::AlignRight | Qt::AlignVCenter,
                         QString::number(yMin + (yMax - yMin) * i / 4, 'g', 5));
        const double column = plot.left() + i * plot.width() / 4;
        const auto time = QDateTime::fromMSecsSinceEpoch(start + span * i / 4, QTimeZone(0));
        if (i == 0 || (duration > 0 && (span > 10000 || i == 4)))
            painter.drawText(QRectF(column - 45, plot.bottom() + 4, 90, 18), Qt::AlignCenter,
                             time.toString(i == 4 || duration == 0 ? "HH:mm:ss 'UTC'" : "HH:mm:ss"));
    }
    qsizetype left = width() - 4;
    for (const auto &series : series_) left -= painter.fontMetrics().horizontalAdvance(series.name) + 44;
    for (const auto &series : series_) {
        painter.setPen(QPen(series.color, 2, series.dashed ? Qt::DashLine : Qt::SolidLine));
        painter.drawLine(QPointF(left, 16), QPointF(left + 18, 16));
        painter.setPen(QColor("#32475e"));
        painter.drawText(QPointF(left + 24, 20), series.name);
        left += painter.fontMetrics().horizontalAdvance(series.name) + 44;
    }
    painter.save();
    painter.setClipRect(plot.adjusted(-2, -2, 2, 2));
    if (selected_.isValid()) {
        const auto column = x(selected_.toMSecsSinceEpoch());
        painter.setPen(QPen(QColor("#93a4b7"), 1, Qt::DashLine));
        painter.drawLine(QPointF(column, plot.top()), QPointF(column, plot.bottom()));
    }
    painter.setRenderHint(QPainter::Antialiasing, false);
    for (const auto &series : series_) {
        QPainterPath path;
        painter.setPen(QPen(series.color, 1.5, series.dashed ? Qt::DashLine : Qt::SolidLine));
        for (qsizetype i = 0; i < series.points.size(); ++i) {
            const auto &current = series.points[i];
            const QPointF pixel(x(current.x()), y(current.y()));
            const bool joinsPrevious = i > 0 && current.x() - series.points[i - 1].x() == 10000;
            if (joinsPrevious) path.lineTo(pixel);
            else path.moveTo(pixel);
            if (!series.dashed && series.points.size() <= 72) {
                painter.setBrush(series.color);
                painter.drawEllipse(pixel, 2.0, 2.0);
            }
        }
        painter.drawPath(path);
    }
    painter.restore();
}

void ScoreChart::mousePressEvent(QMouseEvent *event)
{
    const auto plot = plotRect();
    if (event->button() != Qt::LeftButton || !from_.isValid() || !plot.contains(event->position())) return;
    const auto offset = (event->position().x() - plot.left()) * qMax<qint64>(10000, from_.msecsTo(to_)) / plot.width();
    const auto picked = from_.addMSecs(qRound64(offset / 10000) * 10000);
    emit timeSelected(qMin(picked, to_));
}
