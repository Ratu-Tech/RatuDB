/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.inject.matcher;

import org.opensearch.common.SuppressForbidden;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Matcher implementations. Supports matching classes and methods.
 *
 * @author crazybob@google.com (Bob Lee)
 *
 * @opensearch.internal
 */
public class Matchers {
    private Matchers() {}

    /**
     * Returns a matcher which matches any input.
     */
    public static Matcher<Object> any() {
        return ANY;
    }

    private static final Matcher<Object> ANY = new Any();

    /**
     * Matches ANY
     *
     * @opensearch.internal
     */
    private static class Any extends AbstractMatcher<Object> {
        @Override
        public boolean matches(Object o) {
            return true;
        }

        @Override
        public String toString() {
            return "any()";
        }

        public Object readResolve() {
            return any();
        }
    }

    /**
     * Inverts the given matcher.
     */
    public static <T> Matcher<T> not(final Matcher<? super T> p) {
        return new Not<>(p);
    }

    /**
     * A NOT Matcher
     *
     * @opensearch.internal
     */
    private static class Not<T> extends AbstractMatcher<T> {
        final Matcher<? super T> delegate;

        private Not(Matcher<? super T> delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public boolean matches(T t) {
            return !delegate.matches(t);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Not && ((Not) other).delegate.equals(delegate);
        }

        @Override
        public int hashCode() {
            return -delegate.hashCode();
        }

        @Override
        public String toString() {
            return "not(" + delegate + ")";
        }
    }

    private static void checkForRuntimeRetention(Class<? extends Annotation> annotationType) {
        Retention retention = annotationType.getAnnotation(Retention.class);
        if (retention == null || retention.value() != RetentionPolicy.RUNTIME) {
            throw new IllegalArgumentException("Annotation " + annotationType.getSimpleName() + " is missing RUNTIME retention");
        }
    }

    /**
     * Returns a matcher which matches elements (methods, classes, etc.)
     * with a given annotation.
     */
    public static Matcher<AnnotatedElement> annotatedWith(final Class<? extends Annotation> annotationType) {
        return new AnnotatedWithType(annotationType);
    }

    /**
     * An annotated with type
     *
     * @opensearch.internal
     */
    private static class AnnotatedWithType extends AbstractMatcher<AnnotatedElement> {
        private final Class<? extends Annotation> annotationType;

        AnnotatedWithType(Class<? extends Annotation> annotationType) {
            this.annotationType = Objects.requireNonNull(annotationType, "annotation type");
            checkForRuntimeRetention(annotationType);
        }

        @Override
        public boolean matches(AnnotatedElement element) {
            return element.getAnnotation(annotationType) != null;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof AnnotatedWithType && ((AnnotatedWithType) other).annotationType.equals(annotationType);
        }

        @Override
        public int hashCode() {
            return 37 * annotationType.hashCode();
        }

        @Override
        public String toString() {
            return "annotatedWith(" + annotationType.getSimpleName() + ".class)";
        }
    }

    /**
     * Returns a matcher which matches elements (methods, classes, etc.)
     * with a given annotation.
     */
    public static Matcher<AnnotatedElement> annotatedWith(final Annotation annotation) {
        return new AnnotatedWith(annotation);
    }

    /**
     * An annotated With matcher
     *
     * @opensearch.internal
     */
    private static class AnnotatedWith extends AbstractMatcher<AnnotatedElement> {
        private final Annotation annotation;

        AnnotatedWith(Annotation annotation) {
            this.annotation = Objects.requireNonNull(annotation, "annotation");
            checkForRuntimeRetention(annotation.annotationType());
        }

        @Override
        public boolean matches(AnnotatedElement element) {
            Annotation fromElement = element.getAnnotation(annotation.annotationType());
            return fromElement != null && annotation.equals(fromElement);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof AnnotatedWith && ((AnnotatedWith) other).annotation.equals(annotation);
        }

        @Override
        public int hashCode() {
            return 37 * annotation.hashCode();
        }

        @Override
        public String toString() {
            return "annotatedWith(" + annotation + ")";
        }
    }

    /**
     * Returns a matcher which matches subclasses of the given type (as well as
     * the given type).
     */
    public static Matcher<Class> subclassesOf(final Class<?> superclass) {
        return new SubclassesOf(superclass);
    }

    /**
     * A subclass matcher
     *
     * @opensearch.internal
     */
    private static class SubclassesOf extends AbstractMatcher<Class> {
        private final Class<?> superclass;

        SubclassesOf(Class<?> superclass) {
            this.superclass = Objects.requireNonNull(superclass, "superclass");
        }

        @Override
        public boolean matches(Class subclass) {
            return superclass.isAssignableFrom(subclass);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof SubclassesOf && ((SubclassesOf) other).superclass.equals(superclass);
        }

        @Override
        public int hashCode() {
            return 37 * superclass.hashCode();
        }

        @Override
        public String toString() {
            return "subclassesOf(" + superclass.getSimpleName() + ".class)";
        }
    }

    /**
     * Returns a matcher which matches objects equal to the given object.
     */
    public static Matcher<Object> only(Object value) {
        return new Only(value);
    }

    /**
     * ONLY matcher
     *
     * @opensearch.internal
     */
    private static class Only extends AbstractMatcher<Object> {
        private final Object value;

        Only(Object value) {
            this.value = Objects.requireNonNull(value, "value");
        }

        @Override
        public boolean matches(Object other) {
            return value.equals(other);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Only && ((Only) other).value.equals(value);
        }

        @Override
        public int hashCode() {
            return 37 * value.hashCode();
        }

        @Override
        public String toString() {
            return "only(" + value + ")";
        }
    }

    /**
     * Returns a matcher which matches only the given object.
     */
    public static Matcher<Object> identicalTo(final Object value) {
        return new IdenticalTo(value);
    }

    /**
     * Identical matcher
     *
     * @opensearch.internal
     */
    private static class IdenticalTo extends AbstractMatcher<Object> {
        private final Object value;

        IdenticalTo(Object value) {
            this.value = Objects.requireNonNull(value, "value");
        }

        @Override
        public boolean matches(Object other) {
            return value == other;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof IdenticalTo && ((IdenticalTo) other).value == value;
        }

        @Override
        public int hashCode() {
            return 37 * System.identityHashCode(value);
        }

        @Override
        public String toString() {
            return "identicalTo(" + value + ")";
        }
    }

    /**
     * Returns a matcher which matches classes in the given package. Packages are specific to their
     * classloader, so classes with the same package name may not have the same package at runtime.
     */
    public static Matcher<Class> inPackage(final Package targetPackage) {
        return new InPackage(targetPackage);
    }

    /**
     * In Package matcher
     *
     * @opensearch.internal
     */
    private static class InPackage extends AbstractMatcher<Class> {
        private final transient Package targetPackage;
        private final String packageName;

        InPackage(Package targetPackage) {
            this.targetPackage = Objects.requireNonNull(targetPackage, "package");
            this.packageName = targetPackage.getName();
        }

        @Override
        public boolean matches(Class c) {
            return c.getPackage().equals(targetPackage);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof InPackage && ((InPackage) other).targetPackage.equals(targetPackage);
        }

        @Override
        public int hashCode() {
            return 37 * targetPackage.hashCode();
        }

        @Override
        public String toString() {
            return "inPackage(" + targetPackage.getName() + ")";
        }

        @SuppressForbidden(reason = "ClassLoader.getDefinedPackage not available yet")
        public Object readResolve() {
            // TODO minJava >= 9 : use ClassLoader.getDefinedPackage and remove @SuppressForbidden
            return inPackage(Package.getPackage(packageName));
        }
    }

    /**
     * Returns a matcher which matches classes in the given package and its subpackages. Unlike
     * {@link #inPackage(Package) inPackage()}, this matches classes from any classloader.
     *
     * @since 2.0
     */
    public static Matcher<Class> inSubpackage(final String targetPackageName) {
        return new InSubpackage(targetPackageName);
    }

    /**
     * In Subpackage matcher
     *
     * @opensearch.internal
     */
    private static class InSubpackage extends AbstractMatcher<Class> {
        private final String targetPackageName;

        InSubpackage(String targetPackageName) {
            this.targetPackageName = targetPackageName;
        }

        @Override
        public boolean matches(Class c) {
            String classPackageName = c.getPackage().getName();
            return classPackageName.equals(targetPackageName) || classPackageName.startsWith(targetPackageName + ".");
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof InSubpackage && ((InSubpackage) other).targetPackageName.equals(targetPackageName);
        }

        @Override
        public int hashCode() {
            return 37 * targetPackageName.hashCode();
        }

        @Override
        public String toString() {
            return "inSubpackage(" + targetPackageName + ")";
        }
    }

    /**
     * Returns a matcher which matches methods with matching return types.
     */
    public static Matcher<Method> returns(final Matcher<? super Class<?>> returnType) {
        return new Returns(returnType);
    }

    /**
     * Returns matcher
     *
     * @opensearch.internal
     */
    private static class Returns extends AbstractMatcher<Method> {
        private final Matcher<? super Class<?>> returnType;

        Returns(Matcher<? super Class<?>> returnType) {
            this.returnType = Objects.requireNonNull(returnType, "return type matcher");
        }

        @Override
        public boolean matches(Method m) {
            return returnType.matches(m.getReturnType());
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Returns && ((Returns) other).returnType.equals(returnType);
        }

        @Override
        public int hashCode() {
            return 37 * returnType.hashCode();
        }

        @Override
        public String toString() {
            return "returns(" + returnType + ")";
        }
    }
}
