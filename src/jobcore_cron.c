#include "jobcore.h"
#include <string.h>
#include <stdlib.h>
#include <time.h>

typedef struct { unsigned min[60], hour[24], dom[32], mon[13], dow[7]; } cron_t;
static int field(const char *s, unsigned *v, unsigned n, unsigned lo, unsigned hi) {
    char *copy = (char *)malloc(strlen(s) + 1); char *part; unsigned i;
    (void)n;
    if (!copy) return 0; strcpy(copy, s); for (part = strtok(copy, ","); part; part = strtok(NULL, ",")) {
        unsigned a, b, step = 1; char *dash = strchr(part, '-'); char *slash = strchr(part, '/');
        if (slash) { *slash++ = 0; step = (unsigned)strtoul(slash, NULL, 10); if (!step) { free(copy); return 0; } }
        if (!strcmp(part, "*")) { a = lo; b = hi; }
        else { a = (unsigned)strtoul(part, NULL, 10); b = a; if (dash) { *dash++ = 0; a = (unsigned)strtoul(part, NULL, 10); b = (unsigned)strtoul(dash, NULL, 10); } }
        if (a < lo || b > hi || a > b) { free(copy); return 0; }
        for (i = a; i <= b; i += step) v[i] = 1;
    }
    free(copy); return 1;
}
static int parse(const char *expr, cron_t *c) {
    char buf[256], *p, *f[5]; unsigned i = 0;
    if (!expr || strlen(expr) >= sizeof buf) return 0; strcpy(buf, expr); p = strtok(buf, " \t"); while (p && i < 5) { f[i++] = p; p = strtok(NULL, " \t"); }
    if (i != 5 || p) return 0; memset(c, 0, sizeof *c);
    return field(f[0], c->min, 60, 0, 59) && field(f[1], c->hour, 24, 0, 23) && field(f[2], c->dom, 32, 1, 31) && field(f[3], c->mon, 13, 1, 12) && field(f[4], c->dow, 7, 0, 6);
}
static int64_t days_before_year(int y) { int64_t a = y - 1; return 365 * a + a / 4 - a / 100 + a / 400; }
static int64_t days_civil(int y, unsigned m, unsigned d) { static const unsigned md[] = {0,31,28,31,30,31,30,31,31,30,31,30,31}; int64_t days = days_before_year(y) - days_before_year(1970); unsigned i; for (i = 1; i < m; ++i) days += md[i] + (i == 2 && (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))); return days + d - 1; }
static int bratislava_offset(int64_t utc) { struct tm t; int y, march, october, start_day, end_day; time_t z = (time_t)utc; if (gmtime_s(&t, &z) != 0) return 1; y = t.tm_year + 1900; march = (int)((days_civil(y, 3, 31) + 4) % 7); october = (int)((days_civil(y, 10, 31) + 4) % 7); start_day = 31 - march; end_day = 31 - october; if (utc >= (days_civil(y, 3, (unsigned)start_day) * 86400 + 3600) && utc < (days_civil(y, 10, (unsigned)end_day) * 86400 + 3600)) return 2; return 1; }
jobdb_result_t jobcore_cron_next_fire(const char *expr, const char *timezone, int64_t after, int64_t *out) {
    cron_t c; int64_t t; unsigned offset; struct tm local; time_t z;
    if (!out || !parse(expr, &c) || (!timezone || (strcmp(timezone, "UTC") && strcmp(timezone, "Europe/Bratislava")))) return JOBDB_ERR_INVALID_ARGUMENT;
    t = after - after % 60 + 60;
    for (; t < after + (int64_t)366 * 86400 * 10; t += 60) { offset = !strcmp(timezone, "UTC") ? 0u : (unsigned)bratislava_offset(t); z = (time_t)(t + (int64_t)offset * 3600); if (gmtime_s(&local, &z) != 0) continue; if (c.min[local.tm_min] && c.hour[local.tm_hour] && c.mon[local.tm_mon + 1] && c.dom[local.tm_mday] && c.dow[local.tm_wday]) { *out = t; return JOBDB_OK; } }
    return JOBDB_ERR_LIMIT;
}
