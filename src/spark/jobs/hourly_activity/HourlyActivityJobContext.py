from jobs.JobContext import JobContext

import csv

class HourlyActivityJobContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'activity')

def to_pairs(context, hour):
    context.inc_counter('activity')
    return hour, 1

def analyze(sc):
    print('Running hourly activity calculator')
    context = HourlyActivityJobContext(sc)
    hours = sc.parallelize(list(range(0, 24)))
    pairs = hours.map(lambda hour: to_pairs(context, hour))
    ordered = pairs.sortBy(lambda pair: pair[1], ascending=True)
    print(ordered.collect())
    context.print_accumulators()