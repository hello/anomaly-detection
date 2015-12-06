DATE_FORMAT = '%Y-%m-%d'

def from_db_rows(results, min_threshold=60):
    days = {}

    """
    returns { "2015-11-01" : [1,1,1,1,1,1]}
    """
    for sensor_value, num_samples, day_of in results:

        day = day_of.strftime(DATE_FORMAT)
        hour = day_of.hour
        if num_samples < min_threshold:
            continue

        if day not in days:
            days[day] = [-1] * 6

        days[day][hour] = sensor_value

    for day in days.keys():
        if -1 in days[day]:
            del days[day]

    return days


