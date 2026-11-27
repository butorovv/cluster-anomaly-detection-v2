#pragma once

#include <QIcon>
#include <QColor>
#include <QString>

namespace v2 {
QString style();
QIcon icon(int index, const QColor &color, int size = 18);
}
