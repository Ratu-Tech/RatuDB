package org.apache.ratu.second;///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.ruitu.second;
//
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class AggsUtils {
//
//    /**
//     * 聚合数据
//     */
//    public static Map<Object, Object> aggsData(Aggregate aggs) {
//        if (aggs._kind() == Aggregate.Kind.Sterms) {
//            return terms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.AutoDateHistogram) {
//            return autoDateHistogram(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Avg) {
//            return avg(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.BoxPlot) {
//            return boxPlot(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.BucketMetricValue) {
//            return bucketMetricValue(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Cardinality) {
//            return cardinality(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Children) {
//            return children(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Composite) {
//            return composite(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.SimpleLongValue) {
//            return simpleLongValue(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.DateHistogram) {
//            return dateHistogram(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.DateRange) {
//            return dateRange(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Derivative) {
//            return derivative(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Dterms) {
//            return dterms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.ExtendedStats) {
//            return extendedStats(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.ExtendedStatsBucket) {
//            return extendedStatsBucket(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Filter) {
//            return filter(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Filters) {
//            return filters(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.GeoBounds) {
//            return geoBounds(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.GeoCentroid) {
//            return geoCentroid(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.GeohashGrid) {
//            return geohashGrid(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.GeoLine) {
//            return geoLine(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.GeotileGrid) {
//            return geotitleGrid(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Global) {
//            return global(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.HdrPercentileRanks) {
//            return hdrPercentileRanks(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.HdrPercentiles) {
//            return hdrPercentiles(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Histogram) {
//            return histogram(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Inference) {
//            return inference(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Lterms) {
//            return lterms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.MatrixStats) {
//            return matrixStats(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Max) {
//            return max(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.MedianAbsoluteDeviation) {
//            return medianAbsoluteDeviation(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Min) {
//            return min(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Missing) {
//            return missing(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.MultiTerms) {
//            return multiTerms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Nested) {
//            return nested(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.PercentilesBucket) {
//            return percentilesBucket(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Range) {
//            return range(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Rate) {
//            return rate(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.ReverseNested) {
//            return reverseNested(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Sampler) {
//            return sampler(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.ScriptedMetric) {
//            return scriptedMetric(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Siglterms) {
//            return siglterms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Sigsterms) {
//            return sigsterms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.SimpleValue) {
//            return simpleValue(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Stats) {
//            return stats(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.StatsBucket) {
//            return statsBucket(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Srareterms) {
//            return srareterms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.StringStats) {
//            return stringStats(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Sum) {
//            return sum(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.TdigestPercentileRanks) {
//            return tdigestPercentileRanks(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.TdigestPercentiles) {
//            return tdigestPercentiles(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.TTest) {
//            return tTest(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.TopHits) {
//            return topHits(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.TopMetrics) {
//            return topMetrics(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Umrareterms) {
//            return umrareterms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.Umsigterms) {
//            return umsigterms(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.ValueCount) {
//            return valueCount(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.VariableWidthHistogram) {
//            return variableWidthHistogram(aggs);
//        } else if (aggs._kind() == Aggregate.Kind.WeightedAvg) {
//            return weightedAvg(aggs);
//        }
//        return new HashMap<>();
//    }
//
//    /**
//     * Terms 聚合结果
//     */
//    private static Map<Object, Object> terms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        StringTermsAggregate sterms = aggs.sterms();
//        List<StringTermsBucket> array = sterms.buckets().array();
//        array.stream().forEach(a -> {
//            maps.put(a.key(), a.docCount());
//        });
//        return maps;
//    }
//
//    private static Map<Object, Object> adjacencyMatrix(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    /**
//     * 自动间隔日期直方图聚合
//     */
//    private static Map<Object, Object> autoDateHistogram(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        AutoDateHistogramAggregate autoDateHistogramAggregate = aggs.autoDateHistogram();
//        List<DateHistogramBucket> array = autoDateHistogramAggregate.buckets().array();
//        array.stream().forEach(a -> {
//            maps.put(a.key(), a.docCount());
//        });
//        return maps;
//    }
//
//
//    /**
//     * avg 聚合
//     */
//    private static Map<Object, Object> avg(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        AvgAggregate avg = aggs.avg();
//        double value = avg.value();
//        maps.put("avg", value);
//        return maps;
//    }
//
//
//    /**
//     * BoxPlot 聚合
//     */
//    private static Map<Object, Object> boxPlot(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        BoxPlotAggregate boxPlotAggregate = aggs.boxPlot();
//        maps.put("min", boxPlotAggregate.min());
//        maps.put("max", boxPlotAggregate.max());
//        maps.put("q1", boxPlotAggregate.q1());
//        maps.put("q2", boxPlotAggregate.q2());
//        maps.put("q3", boxPlotAggregate.q3());
//        maps.put("lower", boxPlotAggregate.lower());
//        maps.put("upper", boxPlotAggregate.upper());
//        return maps;
//    }
//
//
//    /**
//     * 复杂聚合 查询 (未完成)
//     */
//    private static Map<Object, Object> bucketMetricValue(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        BucketMetricValueAggregate bucketMetricValueAggregate = aggs.bucketMetricValue();
//
//        double value = bucketMetricValueAggregate.value();
//        maps.put("", value);
//        return maps;
//    }
//
//    /**
//     * cardinality 基数 聚合
//     *
//     * @param aggs
//     * @return
//     */
//    private static Map<Object, Object> cardinality(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        CardinalityAggregate cardinality = aggs.cardinality();
//        maps.put("value", cardinality.value());
//        return maps;
//    }
//
//
//    private static Map<Object, Object> children(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        ChildrenAggregate children = aggs.children();
//        maps.put("doc_count", children.docCount());
//        return maps;
//    }
//
//    /**
//     * 组合分组
//     */
//    private static Map<Object, Object> composite(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        CompositeAggregate composite = aggs.composite();
//        List<CompositeBucket> array = composite.buckets().array();
//        array.stream().forEach(compositeBucket -> {
//            Map<String, JsonData> key = compositeBucket.key();
//            maps.put(key, compositeBucket.docCount());
//        });
//        return maps;
//    }
//
//    /**
//     * 直方图累计基数聚合
//     */
//    private static Map<Object, Object> simpleLongValue(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        CumulativeCardinalityAggregate cumulativeCardinalityAggregate = aggs.simpleLongValue();
//        long value = cumulativeCardinalityAggregate.value();
//        maps.put("value", value);
//        return maps;
//    }
//
//    /**
//     * dateHistogram
//     */
//    private static Map<Object, Object> dateHistogram(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        List<DateHistogramBucket> array = aggs.dateHistogram().buckets().array();
//        array.stream().forEach(date -> {
//            maps.put(date.key(), date.docCount());
//        });
//        return maps;
//    }
//
//
//    /**
//     * dateRange
//     */
//    private static Map<Object, Object> dateRange(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        List<RangeBucket> array = aggs.dateRange().buckets().array();
//        array.stream().forEach(date -> {
//            maps.put(date.key(), date.docCount());
//        });
//        return maps;
//    }
//
//
//    /**
//     * derivative 管道聚合
//     */
//    private static Map<Object, Object> derivative(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        double value = aggs.derivative().value();
//        maps.put("derivative", value);
//        return maps;
//    }
//
//
//    private static Map<Object, Object> dterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        List<DoubleTermsBucket> array = aggs.dterms().buckets().array();
//        array.stream().forEach(d -> {
//            maps.put(d.keyAsString(), d.docCount());
//        });
//        return maps;
//    }
//
//    private static Map<Object, Object> extendedStats(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> extendedStatsBucket(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> filter(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> filters(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> geoBounds(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> geoCentroid(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> geohashGrid(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> geoLine(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> geotitleGrid(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> global(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> hdrPercentileRanks(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> hdrPercentiles(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> histogram(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        List<HistogramBucket> array = aggs.histogram().buckets().array();
//        array.stream().forEach(bucket -> {
//            maps.put(bucket.keyAsString(), bucket.docCount());
//        });
//        return maps;
//    }
//
//    private static Map<Object, Object> inference(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> ipRange(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> lrareterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> lterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> matrixStats(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> max(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        double value = aggs.max().value();
//        maps.put("value", value);
//        return maps;
//    }
//
//    private static Map<Object, Object> medianAbsoluteDeviation(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> min(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        double value = aggs.min().value();
//        maps.put("value", value);
//        return maps;
//    }
//
//    private static Map<Object, Object> missing(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> multiTerms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> nested(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> percentilesBucket(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> range(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> rate(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> reverseNested(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> sampler(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> scriptedMetric(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> siglterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> sigsterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> simpleValue(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> stats(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> statsBucket(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> srareterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> stringStats(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> sterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> sum(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        double value = aggs.sum().value();
//        maps.put("value", value);
//        return maps;
//    }
//
//    private static Map<Object, Object> tdigestPercentileRanks(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> tdigestPercentiles(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> tTest(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> topHits(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> topMetrics(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> umrareterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> umsigterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> umterms(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> valueCount(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        double value = aggs.valueCount().value();
//        maps.put("value", value);
//        return maps;
//    }
//
//    private static Map<Object, Object> variableWidthHistogram(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//    private static Map<Object, Object> weightedAvg(Aggregate aggs) {
//        Map<Object, Object> maps = new HashMap<>();
//        return maps;
//    }
//
//
//}
