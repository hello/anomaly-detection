DATE_FORMAT = '%Y-%m-%d'

def from_db_rows(results, min_threshold=60):
    days = {}

    """
    returns { "2015-11-01" : [0,0,0,0,0,0]}
    """
    for sensor_value, num_samples, day_of in results:

        day = day_of.strftime(DATE_FORMAT)
        hour = day_of.hour
        if num_samples < min_threshold:
            continue

        if day not in days:
            days[day] = [-1] * 6

        days[day][hour] = sensor_value

    return days


