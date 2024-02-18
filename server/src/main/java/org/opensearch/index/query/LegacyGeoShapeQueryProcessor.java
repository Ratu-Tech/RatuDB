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

package org.opensearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.opensearch.OpenSearchException;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.SpatialStrategy;
import org.opensearch.common.geo.builders.CircleBuilder;
import org.opensearch.common.geo.builders.EnvelopeBuilder;
import org.opensearch.common.geo.builders.GeometryCollectionBuilder;
import org.opensearch.common.geo.builders.LineStringBuilder;
import org.opensearch.common.geo.builders.MultiLineStringBuilder;
import org.opensearch.common.geo.builders.MultiPointBuilder;
import org.opensearch.common.geo.builders.MultiPolygonBuilder;
import org.opensearch.common.geo.builders.PointBuilder;
import org.opensearch.common.geo.builders.PolygonBuilder;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.geometry.Circle;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.GeometryVisitor;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.MultiPoint;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.opensearch.index.mapper.LegacyGeoShapeFieldMapper;

import java.util.ArrayList;
import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Shape;

import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Query Processor for Legacy Geo Shape types (prefix trees)
 *
 * @opensearch.internal
 *
 * @deprecated this will be removed in a future release
 */
@Deprecated
public class LegacyGeoShapeQueryProcessor {

    private AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType ft;

    public LegacyGeoShapeQueryProcessor(AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType ft) {
        this.ft = ft;
    }

    public Query geoShapeQuery(
        Geometry shape,
        String fieldName,
        SpatialStrategy strategy,
        ShapeRelation relation,
        QueryShardContext context
    ) {
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when '"
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "' is set to false."
            );
        }

        LegacyGeoShapeFieldMapper.GeoShapeFieldType shapeFieldType = (LegacyGeoShapeFieldMapper.GeoShapeFieldType) ft;
        SpatialStrategy spatialStrategy = shapeFieldType.strategy();
        if (strategy != null) {
            spatialStrategy = strategy;
        }
        PrefixTreeStrategy prefixTreeStrategy = shapeFieldType.resolvePrefixTreeStrategy(spatialStrategy);
        if (prefixTreeStrategy instanceof RecursivePrefixTreeStrategy && relation == ShapeRelation.DISJOINT) {
            // this strategy doesn't support disjoint anymore: but it did
            // before, including creating lucene fieldcache (!)
            // in this case, execute disjoint as exists && !intersects
            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            Query exists = ExistsQueryBuilder.newFilter(context, fieldName, false);
            Query intersects = prefixTreeStrategy.makeQuery(getArgs(shape, ShapeRelation.INTERSECTS));
            bool.add(exists, BooleanClause.Occur.MUST);
            bool.add(intersects, BooleanClause.Occur.MUST_NOT);
            return bool.build();
        } else {
            return prefixTreeStrategy.makeQuery(getArgs(shape, relation));
        }
    }

    public static SpatialArgs getArgs(Geometry shape, ShapeRelation relation) {
        switch (relation) {
            case DISJOINT:
                return new SpatialArgs(SpatialOperation.IsDisjointTo, buildS4J(shape));
            case INTERSECTS:
                return new SpatialArgs(SpatialOperation.Intersects, buildS4J(shape));
            case WITHIN:
                return new SpatialArgs(SpatialOperation.IsWithin, buildS4J(shape));
            case CONTAINS:
                return new SpatialArgs(SpatialOperation.Contains, buildS4J(shape));
            default:
                throw new IllegalArgumentException("invalid relation [" + relation + "]");
        }
    }

    /**
     * Builds JTS shape from a geometry
     * <p>
     * This method is needed to handle legacy indices and will be removed when we no longer need to build JTS shapes
     */
    private static Shape buildS4J(Geometry geometry) {
        return geometryToShapeBuilder(geometry).buildS4J();
    }

    public static ShapeBuilder<?, ?, ?> geometryToShapeBuilder(Geometry geometry) {
        ShapeBuilder<?, ?, ?> shapeBuilder = geometry.visit(new GeometryVisitor<ShapeBuilder<?, ?, ?>, RuntimeException>() {
            @Override
            public ShapeBuilder<?, ?, ?> visit(Circle circle) {
                return new CircleBuilder().center(circle.getLon(), circle.getLat()).radius(circle.getRadiusMeters(), DistanceUnit.METERS);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(GeometryCollection<?> collection) {
                GeometryCollectionBuilder shapes = new GeometryCollectionBuilder();
                for (Geometry geometry : collection) {
                    shapes.shape(geometry.visit(this));
                }
                return shapes;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Line line) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < line.length(); i++) {
                    coordinates.add(new Coordinate(line.getX(i), line.getY(i), line.getZ(i)));
                }
                return new LineStringBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(LinearRing ring) {
                throw new UnsupportedOperationException("circle is not supported");
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiLine multiLine) {
                MultiLineStringBuilder lines = new MultiLineStringBuilder();
                for (int i = 0; i < multiLine.size(); i++) {
                    lines.linestring((LineStringBuilder) visit(multiLine.get(i)));
                }
                return lines;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPoint multiPoint) {
                List<Coordinate> coordinates = new ArrayList<>();
                for (int i = 0; i < multiPoint.size(); i++) {
                    Point p = multiPoint.get(i);
                    coordinates.add(new Coordinate(p.getX(), p.getY(), p.getZ()));
                }
                return new MultiPointBuilder(coordinates);
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(MultiPolygon multiPolygon) {
                MultiPolygonBuilder polygons = new MultiPolygonBuilder();
                for (int i = 0; i < multiPolygon.size(); i++) {
                    polygons.polygon((PolygonBuilder) visit(multiPolygon.get(i)));
                }
                return polygons;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Point point) {
                return new PointBuilder(point.getX(), point.getY());
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Polygon polygon) {
                PolygonBuilder polygonBuilder = new PolygonBuilder(
                    (LineStringBuilder) visit((Line) polygon.getPolygon()),
                    ShapeBuilder.Orientation.RIGHT,
                    false
                );
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    polygonBuilder.hole((LineStringBuilder) visit((Line) polygon.getHole(i)));
                }
                return polygonBuilder;
            }

            @Override
            public ShapeBuilder<?, ?, ?> visit(Rectangle rectangle) {
                return new EnvelopeBuilder(
                    new Coordinate(rectangle.getMinX(), rectangle.getMaxY()),
                    new Coordinate(rectangle.getMaxX(), rectangle.getMinY())
                );
            }
        });
        return shapeBuilder;
    }
}
