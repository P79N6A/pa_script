#coding:utf-8

__author__ = "Xu Tao"

import clr
clr.AddReference("safe_read_sqlite")
del clr

import PA_runtime
from PA_runtime import *
from safe_read_sqlite import SqliteByCSharp

from PA.InfraLib.ModelsV2 import *

class AppleHealth(object):

    def __init__(self, node, extractDeleted, extractSource):
        self.root = node
        self.extractDeleted = extractDeleted
        self.extractSource = extractSource
        self.cache = ds.OpenCachePath("AppleHealth")

    
    def parse(self):
        results = []

        results.extend(self.get_heart_rate())
        results.extend(self.get_weight())
        results.extend(self.get_fitness_record())
        results.extend(self.get_spcific_step())
        results.extend(self.get_spcific_distance())

        return results

    
    def get_heart_rate(self):
        models = []
        if self.root is None:
            return
        try:
            conn = SqliteByCSharp(self.root, self.cache)
            with conn as cmd:
                
                cmd.CommandText = """
                    select (samples.start_date+978336000) as "Start Date",(samples.end_date+978336000) as "End Date", samples.data_id,data_type,
                        quantity,original_quantity,unit_strings.unit_string,data_provenances.origin_product_type,data_provenances.origin_build,data_provenances.local_product_type,data_provenances.local_build 
                        from samples 
                            left outer join quantity_samples on samples.data_id = quantity_samples.data_id 
                            left outer join unit_strings on quantity_samples.original_unit = unit_strings.RowID 
                            left outer join objects on samples.data_id = objects.data_id 
                            left outer join data_provenances on objects.provenance = data_provenances.RowID 
                            where data_type = 5
                """
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        cache_time = SqliteByCSharp.GetFloat(reader, 1)
                        rate = SqliteByCSharp.GetFloat(reader, 5)
                        # origin_device = SqliteByCSharp.GetString(reader, 7)
                        # local_device = SqliteByCSharp.GetString(reader, 9)
                        heart_rate = Health.System.HeartRateRecord()
                        heart_rate.Time = TimeStamp.FromUnixTime(cache_time, False)
                        heart_rate.HeartRate = float(rate)

                        models.append(heart_rate)
                    except Exception as e:
                        print(e)
        except Exception as e:
            pass
        return models
        

    def get_weight(self):
        models = []
        if self.root is None:
            return
        try:
            conn = SqliteByCSharp(self.root, self.cache)
            with conn as cmd:
                
                cmd.CommandText = """
                    select (samples.start_date+978336000) as "Start Date",(samples.end_date+978336000) as "End Date", samples.data_id,data_type,
                        quantity,original_quantity,unit_strings.unit_string,data_provenances.origin_product_type,data_provenances.origin_build,data_provenances.local_product_type,data_provenances.local_build 
                        from samples 
                            left outer join quantity_samples on samples.data_id = quantity_samples.data_id 
                            left outer join unit_strings on quantity_samples.original_unit = unit_strings.RowID 
                            left outer join objects on samples.data_id = objects.data_id 
                            left outer join data_provenances on objects.provenance = data_provenances.RowID 
                            where data_type = 3
                """
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        cache_time = SqliteByCSharp.GetFloat(reader, 1)
                        weights = SqliteByCSharp.GetFloat(reader, 4)
                        # origin_device = SqliteByCSharp.GetString(reader, 7)
                        # local_device = SqliteByCSharp.GetString(reader, 9)

                        weight = Health.System.WeightRecord()
                        weight.Time = TimeStamp.FromUnixTime(cache_time, False)
                        weight.Weight = weights

                        models.append(weight)
                    except Exception as e:
                        print(e)
        except Exception as e:
            pass
        return models


    def get_spcific_step(self):
        models = []
        if self.root is None:
            return
        try:
            conn = SqliteByCSharp(self.root, self.cache)
            with conn as cmd:
                
                cmd.CommandText = """
                    select (samples.start_date+978336000) as "Start Date",(samples.end_date+978336000) as "End Date", samples.data_id,data_type,
                        quantity,original_quantity,unit_strings.unit_string,data_provenances.origin_product_type,data_provenances.origin_build,data_provenances.local_product_type,data_provenances.local_build 
                        from samples 
                            left outer join quantity_samples on samples.data_id = quantity_samples.data_id 
                            left outer join unit_strings on quantity_samples.original_unit = unit_strings.RowID 
                            left outer join objects on samples.data_id = objects.data_id 
                            left outer join data_provenances on objects.provenance = data_provenances.RowID 
                            where data_type = 7
                """
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        cache_time = SqliteByCSharp.GetFloat(reader, 1)
                        step = SqliteByCSharp.GetFloat(reader, 4)

                        spcific_step = Health.System.StepRecord()
                        spcific_step.Time = TimeStamp.FromUnixTime(cache_time, False)
                        spcific_step.Number = step

                        models.append(spcific_step)
                    except Exception as e:
                        print(e)
        except Exception as e:
            pass
        return models


    def get_spcific_distance(self):
        models = []
        if self.root is None:
            return
        try:
            conn = SqliteByCSharp(self.root, self.cache)
            with conn as cmd:
                
                cmd.CommandText = """
                    select (samples.start_date+978336000) as "Start Date",(samples.end_date+978336000) as "End Date", samples.data_id,data_type,
                        quantity,original_quantity,unit_strings.unit_string,data_provenances.origin_product_type,data_provenances.origin_build,data_provenances.local_product_type,data_provenances.local_build 
                        from samples 
                            left outer join quantity_samples on samples.data_id = quantity_samples.data_id 
                            left outer join unit_strings on quantity_samples.original_unit = unit_strings.RowID 
                            left outer join objects on samples.data_id = objects.data_id 
                            left outer join data_provenances on objects.provenance = data_provenances.RowID 
                            where data_type = 8
                """
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        cache_time = SqliteByCSharp.GetFloat(reader, 1)
                        distance = SqliteByCSharp.GetFloat(reader, 4)
                        
                        spcific_distance = Health.System.DistanceRecord()
                        spcific_distance.Time = TimeStamp.FromUnixTime(cache_time, False)
                        spcific_distance.Distance = float(distance)

                        models.append(spcific_distance)
                    except Exception as e:
                        print(e)
        except Exception as e:
            pass
        return models
    
    def get_fitness_record(self):
        models = []
        if self.root is None:
            return
        try:
            conn = SqliteByCSharp(self.root, self.cache)
            with conn as cmd:
                
                cmd.CommandText = """
                    select (energy_burned_goal_date+978336000) as "Energy Burned Goal Date", 
                        (cache_index+978336000) as "cache_index", 
                        energy_burned,energy_burned_goal,active_hours,active_hours_goal,brisk_minutes,brisk_minutes_goal,
                        steps,walk_distance from activity_caches
                """
                reader = cmd.ExecuteReader()
                while reader.Read():
                    try:
                        cache_time = SqliteByCSharp.GetFloat(reader, 1)
                        energy_burned = SqliteByCSharp.GetFloat(reader, 2)
                        # energy_burned_goal = SqliteByCSharp.GetFloat(reader, 3)
                        active_hours = SqliteByCSharp.GetFloat(reader, 4)
                        # active_hours_goal = SqliteByCSharp.GetFloat(reader, 5)
                        brisk_minutes = SqliteByCSharp.GetFloat(reader, 6)
                        # brisk_minutes_goal = SqliteByCSharp.GetFloat(reader, 7)
                        step = SqliteByCSharp.GetFloat(reader, 8)
                        walk_distance = SqliteByCSharp.GetFloat(reader, 9)

                        fitness_record = Health.System.FitnessRecord()
                        fitness_record.Activity = energy_burned
                        fitness_record.Workout = brisk_minutes
                        fitness_record.Standing = active_hours
                        fitness_record.StepNumber = step
                        fitness_record.Distance = walk_distance
                        fitness_record.Time = TimeStamp.FromUnixTime(cache_time, False)

                        models.append(fitness_record)
                    except Exception as e:
                        print(e)
        except Exception as e:
            pass
        return models


def analyze_health(node, extractDeleted, extractSource):
    pr = ParserResults()
    results = AppleHealth(node, extractDeleted, extractSource).parse()
    if results:
        pr.Models.AddRange(results)
        pr.Build("健康")
    return pr