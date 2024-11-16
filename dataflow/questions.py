import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from datetime import datetime,timedelta
from apache_beam.transforms.combiners import Mean
import os
from datetime import datetime

import os
# Configure logging

if __name__ == "__main__":
    try:
        # Set environment variable for Google credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\harsh\Downloads\Study\Spark Lectures\Projects\earthquake_ingesion_hp\earthquake-analysis-440806-e4fcdf0763f4.json'
        
        options = PipelineOptions()
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = "earthquake-analysis-440806"
        google_cloud_options.job_name = "DataflowQuetions"
        google_cloud_options.region = "us-central1"
        google_cloud_options.staging_location = "gs://dataproc-staging-us-central1-1041991067679-wndc42dq/stage_loc/"
        google_cloud_options.temp_location = "gs://dataproc-temp-us-central1-1041991067679-qkiizwmb/temp_loc/"


        def avg_mag(element):
            value = element[1]
            length = len(value)
            addition = sum(value)
            avg = addition/length
            return (element[0],avg)

        def avg_per_day_location(element):
            value = element[1]
            group = element[0]
            length = len(value)
            addition = sum(value)
            avg = addition / length

            return (group,avg)


        # class ExtractDate(beam.DoFn):
        #     def process(self, element):
        #         # Access the timestamp from the dictionary
        #         timestamp = element['time']
        #         if timestamp.tzinfo is not None:
        #             timestamp = timestamp.replace(tzinfo=None)
        #
        #         # Calculate last week's datetime, offset-naive
        #         last_week = datetime.now().replace(tzinfo=None) - timedelta(days=7)
        #
        #         # Filter events from the last week
        #         if timestamp >= last_week:
        #             yield element


        class FilterLastWeek(beam.DoFn):
            def process(self, element):
                # Assuming each element has a 'time' field in datetime format and 'region' & 'magnitude' fields
                timestamp = element['time']  # Adjust field name if necessary
                last_week = datetime.now().replace(tzinfo=None) - timedelta(days=7)

                # Filter events from the last week
                if timestamp >= last_week:
                    yield element




        with beam.Pipeline(options=options) as p:
            query = """
                select area,time,mag from `earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet` 
                     """
            data_from_bq = (
                             p | 'read' >> beam.io.gcp.bigquery.ReadFromBigQuery(query=query,use_standard_sql = True)

                )

            #1. Count the number of earthquakes by region
            # region_count = (
            #     data_from_bq
            #     | 'region_selection' >> beam.Map(lambda x:(x['area'],1))
            #     | 'count of region' >> beam.CombinePerKey(sum)
            #     |'show1' >> beam.Map(print)
            # )

            #2. Find the average magnitude by the region
            # avg_mag_region = (
            #     data_from_bq
            #     | 'col select' >> beam.Map(lambda x:(x['area'],x['mag']))
            #     | 'group by key' >> beam.GroupByKey()
            #     | 'avg' >> beam.Map(avg_mag)
            #     | 'show2' >> beam.Map(print)
            #
            # )

            #3. Find how many earthquakes happen on the same day.
            # same_day_earthquake = (
            #     data_from_bq
            #     | 'specific date' >> beam.ParDo(ExtractDate())
            #     | 'col select 1' >> beam.Map(lambda x:(x['date'],1))
            #     | 'earthquake on each day' >> beam.CombinePerKey(sum)
            #     | 'show3' >> beam.Map(print)
            #
            # )

            #4. Find how many earthquakes happen on same day and in same region
            # region_day_wise = (
            #     data_from_bq
            #     | 'specific date' >> beam.ParDo(ExtractDate())
            #     | 'col select 2' >> beam.Map( lambda x:((x['area'],x['date']), 1))
            #     |  'earthquake on each day and region' >> beam.CombinePerKey(sum)
            #     | 'show4' >> beam.Map(print)
            #
            #
            # )

            #5. Find average earthquakes happen on the same day.

            # avg_perday_earthquake = (
            #     data_from_bq
            #     | 'Extract date' >> beam.ParDo(ExtractDate())
            #     | 'Map to (date, 1)' >> beam.Map(lambda x: (x['date'], 1))
            #     | 'Count earthquakes per day' >> beam.CombinePerKey(sum)
            #     | 'Get daily counts' >> beam.Values()
            #      | 'Calculate average' >> Mean.Globally()
            #     | 'Print results' >> beam.Map(print)
            # )

            #6. Find average earthquakes happen on same day and in same region

        #     same_day_area_avg_earthquake = (
        #         data_from_bq
        #         | 'specific date1' >> beam.ParDo(ExtractDate())
        #         | 'col select 5' >> beam.Map(lambda x:((x['area'],x['date']),1))
        #         | 'group by key' >> beam.GroupByKey()
        #         | 'avg per area and date' >>  beam.Map(avg_per_day_location)
        #
        #         | 'show' >> beam.Map(print)
        #
        # )

    #         #7 Find the region name, which had the highest magnitude earthquake last week.
    #         high_mag_earth_last_week = (
    #                  data_from_bq
    #     | 'Filter last week' >> beam.ParDo(FilterLastWeek())
    #     | 'Map to (region, magnitude)' >> beam.Map(lambda x: (x['area'], x['mag']))
    #     # | 'Find max magnitude per region' >> beam.CombinePerKey(max)
    #     # | 'Find region with highest magnitude' >> beam.CombineGlobally(
    #     #     lambda region_magnitude_pairs: max(region_magnitude_pairs, key=lambda x: x[1]))
    #     | 'Print results1' >> beam.Map(print)
    #
    # )

        #8. Find the region name, which is having magnitudes higher than 5.
        #     mag_grt_5 = (
        #         data_from_bq
        #     | 'filter_data' >> beam.Filter(lambda x: x['mag']>5)
        #     | 'region' >> beam.Map(lambda x:(x['area'],x['mag']))
        #     | 'Print results2' >> beam.Map(print)
        # )

        #9. Find out the regions which are having the highest frequency and intensity of earthquakes.

            high_frqn = (
                    data_from_bq
                    | 'Map to (region, 1)' >> beam.Map(lambda x: (x['area'], 1))  # Map each row to (region, 1)
                    | 'Count earthquakes per region' >> beam.CombinePerKey(
                sum)  # Count the number of earthquakes per region
                    | 'Find region with max frequency' >> beam.CombineGlobally(
                # The lambda function here should find the region with the maximum frequency
                lambda region_count_pairs: max(region_count_pairs, key=lambda x: x[1])
            )
                    | 'Print results3' >> beam.Map(print)
            )

            high_intensity = (
                    data_from_bq
                    | 'Map to (region, magnitude)' >> beam.Map(lambda x: (x['area'], x['mag']))
                    | 'Find max magnitude per region' >> beam.CombinePerKey(max)
                    | 'Find region with max1 frequency' >> beam.CombineGlobally(
                lambda region_count_pairs: max(region_count_pairs, key=lambda x: x[1])
            )

                    | 'Print results4' >> beam.Map(print)
            )




    except Exception as e:
        print(f"Pipeline failed: {e}")