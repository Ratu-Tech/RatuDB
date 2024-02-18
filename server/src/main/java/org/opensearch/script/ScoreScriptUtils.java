/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.script;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.geo.GeoDistance;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.mapper.DateFieldMapper;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.opensearch.common.util.BitMixer.mix32;
import static org.opensearch.index.query.functionscore.TermFrequencyFunctionFactory.TermFrequencyFunctionName.SUM_TOTAL_TERM_FREQ;
import static org.opensearch.index.query.functionscore.TermFrequencyFunctionFactory.TermFrequencyFunctionName.TERM_FREQ;
import static org.opensearch.index.query.functionscore.TermFrequencyFunctionFactory.TermFrequencyFunctionName.TOTAL_TERM_FREQ;

/**
 * Utilities for scoring scripts
 *
 * @opensearch.internal
 */
public final class ScoreScriptUtils {

    /****** STATIC FUNCTIONS that can be used by users for score calculations **/

    public static double saturation(double value, double k) {
        return value / (k + value);
    }

    /**
     * Calculate a sigmoid of <code>value</code>
     * with scaling parameters <code>k</code> and <code>a</code>
     */
    public static double sigmoid(double value, double k, double a) {
        return Math.pow(value, a) / (Math.pow(k, a) + Math.pow(value, a));
    }

    /**
     * Retrieves the term frequency within a field for a specific term.
     *
     * @opensearch.internal
     */
    public static final class TermFreq {
        private final ScoreScript scoreScript;

        public TermFreq(ScoreScript scoreScript) {
            this.scoreScript = scoreScript;
        }

        public int termFreq(String field, String term) {
            try {
                return (int) scoreScript.getTermFrequency(TERM_FREQ, field, term);
            } catch (Exception e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }
    }

    /**
     * Retrieves the total term frequency within a field for a specific term.
     *
     * @opensearch.internal
     */
    public static final class TotalTermFreq {
        private final ScoreScript scoreScript;

        public TotalTermFreq(ScoreScript scoreScript) {
            this.scoreScript = scoreScript;
        }

        public long totalTermFreq(String field, String term) {
            try {
                return (long) scoreScript.getTermFrequency(TOTAL_TERM_FREQ, field, term);
            } catch (Exception e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }
    }

    /**
     * Retrieves the sum of total term frequencies within a field.
     *
     * @opensearch.internal
     */
    public static final class SumTotalTermFreq {
        private final ScoreScript scoreScript;

        public SumTotalTermFreq(ScoreScript scoreScript) {
            this.scoreScript = scoreScript;
        }

        public long sumTotalTermFreq(String field) {
            try {
                return (long) scoreScript.getTermFrequency(SUM_TOTAL_TERM_FREQ, field, null);
            } catch (Exception e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }
    }

    /**
     * random score based on the documents' values of the given field
     *
     * @opensearch.internal
     */
    public static final class RandomScoreField {
        private final ScoreScript scoreScript;
        private final ScriptDocValues docValues;
        private final int saltedSeed;

        public RandomScoreField(ScoreScript scoreScript, int seed, String fieldName) {
            this.scoreScript = scoreScript;
            this.docValues = scoreScript.getDoc().get(fieldName);
            int salt = (scoreScript._getIndex().hashCode() << 10) | scoreScript._getShardId();
            this.saltedSeed = mix32(salt ^ seed);

        }

        public double randomScore() {
            try {
                docValues.setNextDocId(scoreScript._getDocId());
                String seedValue = String.valueOf(docValues.get(0));
                int hash = StringHelper.murmurhash3_x86_32(new BytesRef(seedValue), saltedSeed);
                return (hash & 0x00FFFFFF) / (float) (1 << 24); // only use the lower 24 bits to construct a float from 0.0-1.0
            } catch (Exception e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }
    }

    /**
     * random score based on the internal Lucene document Ids
     *
     * @opensearch.internal
     */
    public static final class RandomScoreDoc {
        private final ScoreScript scoreScript;
        private final int saltedSeed;

        public RandomScoreDoc(ScoreScript scoreScript, int seed) {
            this.scoreScript = scoreScript;
            int salt = (scoreScript._getIndex().hashCode() << 10) | scoreScript._getShardId();
            this.saltedSeed = mix32(salt ^ seed);
        }

        public double randomScore() {
            String seedValue = Integer.toString(scoreScript._getDocBaseId());
            int hash = StringHelper.murmurhash3_x86_32(new BytesRef(seedValue), saltedSeed);
            return (hash & 0x00FFFFFF) / (float) (1 << 24); // only use the lower 24 bits to construct a float from 0.0-1.0
        }
    }

    /**
     * **** Decay functions on geo field
     *
     * @opensearch.internal
     */
    public static final class DecayGeoLinear {
        // cached variables calculated once per script execution
        double originLat;
        double originLon;
        double offset;
        double scaling;

        public DecayGeoLinear(String originStr, String scaleStr, String offsetStr, double decay) {
            GeoPoint origin = GeoUtils.parseGeoPoint(originStr, false);
            double scale = DistanceUnit.DEFAULT.parse(scaleStr, DistanceUnit.DEFAULT);
            this.originLat = origin.lat();
            this.originLon = origin.lon();
            this.offset = DistanceUnit.DEFAULT.parse(offsetStr, DistanceUnit.DEFAULT);
            this.scaling = scale / (1.0 - decay);
        }

        public double decayGeoLinear(GeoPoint docValue) {
            double distance = GeoDistance.ARC.calculate(originLat, originLon, docValue.lat(), docValue.lon(), DistanceUnit.METERS);
            distance = Math.max(0.0d, distance - offset);
            return Math.max(0.0, (scaling - distance) / scaling);
        }
    }

    /**
     * Exponential geo decay
     *
     * @opensearch.internal
     */
    public static final class DecayGeoExp {
        double originLat;
        double originLon;
        double offset;
        double scaling;

        public DecayGeoExp(String originStr, String scaleStr, String offsetStr, double decay) {
            GeoPoint origin = GeoUtils.parseGeoPoint(originStr, false);
            double scale = DistanceUnit.DEFAULT.parse(scaleStr, DistanceUnit.DEFAULT);
            this.originLat = origin.lat();
            this.originLon = origin.lon();
            this.offset = DistanceUnit.DEFAULT.parse(offsetStr, DistanceUnit.DEFAULT);
            this.scaling = Math.log(decay) / scale;
        }

        public double decayGeoExp(GeoPoint docValue) {
            double distance = GeoDistance.ARC.calculate(originLat, originLon, docValue.lat(), docValue.lon(), DistanceUnit.METERS);
            distance = Math.max(0.0d, distance - offset);
            return Math.exp(scaling * distance);
        }
    }

    /**
     * Gaussian geo decay
     *
     * @opensearch.internal
     */
    public static final class DecayGeoGauss {
        double originLat;
        double originLon;
        double offset;
        double scaling;

        public DecayGeoGauss(String originStr, String scaleStr, String offsetStr, double decay) {
            GeoPoint origin = GeoUtils.parseGeoPoint(originStr, false);
            double scale = DistanceUnit.DEFAULT.parse(scaleStr, DistanceUnit.DEFAULT);
            this.originLat = origin.lat();
            this.originLon = origin.lon();
            this.offset = DistanceUnit.DEFAULT.parse(offsetStr, DistanceUnit.DEFAULT);
            this.scaling = 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
        }

        public double decayGeoGauss(GeoPoint docValue) {
            double distance = GeoDistance.ARC.calculate(originLat, originLon, docValue.lat(), docValue.lon(), DistanceUnit.METERS);
            distance = Math.max(0.0d, distance - offset);
            return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
        }
    }

    // **** Decay functions on numeric field

    /**
     * Linear numeric decay
     *
     * @opensearch.internal
     */
    public static final class DecayNumericLinear {
        double origin;
        double offset;
        double scaling;

        public DecayNumericLinear(double origin, double scale, double offset, double decay) {
            this.origin = origin;
            this.offset = offset;
            this.scaling = scale / (1.0 - decay);
        }

        public double decayNumericLinear(double docValue) {
            double distance = Math.max(0.0d, Math.abs(docValue - origin) - offset);
            return Math.max(0.0, (scaling - distance) / scaling);
        }
    }

    /**
     * Exponential numeric decay
     *
     * @opensearch.internal
     */
    public static final class DecayNumericExp {
        double origin;
        double offset;
        double scaling;

        public DecayNumericExp(double origin, double scale, double offset, double decay) {
            this.origin = origin;
            this.offset = offset;
            this.scaling = Math.log(decay) / scale;
        }

        public double decayNumericExp(double docValue) {
            double distance = Math.max(0.0d, Math.abs(docValue - origin) - offset);
            return Math.exp(scaling * distance);
        }
    }

    /**
     * Gaussian numeric decay
     *
     * @opensearch.internal
     */
    public static final class DecayNumericGauss {
        double origin;
        double offset;
        double scaling;

        public DecayNumericGauss(double origin, double scale, double offset, double decay) {
            this.origin = origin;
            this.offset = offset;
            this.scaling = 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
        }

        public double decayNumericGauss(double docValue) {
            double distance = Math.max(0.0d, Math.abs(docValue - origin) - offset);
            return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
        }
    }

    // **** Decay functions on date field

    /**
     * Limitations: since script functions don't have access to DateFieldMapper,
     * decay functions on dates are limited to dates in the default format and default time zone,
     * Further, since script module gets initialized before the featureflags are loaded,
     * we cannot use the feature flag to gate the usage of the new default date format.
     * Also, using calculations with <code>now</code> are not allowed.
     *
     */
    private static final ZoneId defaultZoneId = ZoneId.of("UTC");
    // ToDo: use new default date formatter once feature flag is removed
    private static final DateMathParser dateParser = DateFieldMapper.LEGACY_DEFAULT_DATE_TIME_FORMATTER.toDateMathParser();

    /**
     * Linear date decay
     *
     * @opensearch.internal
     */
    public static final class DecayDateLinear {
        long origin;
        long offset;
        double scaling;

        public DecayDateLinear(String originStr, String scaleStr, String offsetStr, double decay) {
            this.origin = dateParser.parse(originStr, null, false, defaultZoneId).toEpochMilli();
            long scale = TimeValue.parseTimeValue(scaleStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".scale")
                .getMillis();
            this.offset = TimeValue.parseTimeValue(offsetStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".offset")
                .getMillis();
            this.scaling = scale / (1.0 - decay);
        }

        public double decayDateLinear(ZonedDateTime docValueDate) {
            long docValue = docValueDate.toInstant().toEpochMilli();
            // as java.lang.Math#abs(long) is a forbidden API, have to use this comparison instead
            long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
            long distance = Math.max(0, diff - offset);
            return Math.max(0.0, (scaling - distance) / scaling);
        }
    }

    /**
     * Exponential date decay
     *
     * @opensearch.internal
     */
    public static final class DecayDateExp {
        long origin;
        long offset;
        double scaling;

        public DecayDateExp(String originStr, String scaleStr, String offsetStr, double decay) {
            this.origin = dateParser.parse(originStr, null, false, defaultZoneId).toEpochMilli();
            long scale = TimeValue.parseTimeValue(scaleStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".scale")
                .getMillis();
            this.offset = TimeValue.parseTimeValue(offsetStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".offset")
                .getMillis();
            this.scaling = Math.log(decay) / scale;
        }

        public double decayDateExp(ZonedDateTime docValueDate) {
            long docValue = docValueDate.toInstant().toEpochMilli();
            long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
            long distance = Math.max(0, diff - offset);
            return Math.exp(scaling * distance);
        }
    }

    /**
     * Gaussian date decay
     *
     * @opensearch.internal
     */
    public static final class DecayDateGauss {
        long origin;
        long offset;
        double scaling;

        public DecayDateGauss(String originStr, String scaleStr, String offsetStr, double decay) {
            this.origin = dateParser.parse(originStr, null, false, defaultZoneId).toEpochMilli();
            long scale = TimeValue.parseTimeValue(scaleStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".scale")
                .getMillis();
            this.offset = TimeValue.parseTimeValue(offsetStr, TimeValue.timeValueHours(24), getClass().getSimpleName() + ".offset")
                .getMillis();
            this.scaling = 0.5 * Math.pow(scale, 2.0) / Math.log(decay);
        }

        public double decayDateGauss(ZonedDateTime docValueDate) {
            long docValue = docValueDate.toInstant().toEpochMilli();
            long diff = (docValue >= origin) ? (docValue - origin) : (origin - docValue);
            long distance = Math.max(0, diff - offset);
            return Math.exp(0.5 * Math.pow(distance, 2.0) / scaling);
        }
    }
}
