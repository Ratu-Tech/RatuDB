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

package org.opensearch.painless.spi;

import org.opensearch.painless.spi.annotation.AllowlistAnnotationParser;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Loads and creates a {@link Allowlist} from one to many text files. */
public final class AllowlistLoader {

    /**
     * Loads and creates a {@link Allowlist} from one to many text files using only the base annotation parsers.
     * See {@link #loadFromResourceFiles(Class, Map, String...)} for information on how to structure an allowlist
     * text file.
     */
    public static Allowlist loadFromResourceFiles(Class<?> resource, String... filepaths) {
        return loadFromResourceFiles(resource, AllowlistAnnotationParser.BASE_ANNOTATION_PARSERS, filepaths);
    }

    /**
     * Loads and creates a {@link Allowlist} from one to many text files. The file paths are passed in as an array of
     * {@link String}s with a single {@link Class} to be be used to load the resources where each {@link String}
     * is the path of a single text file. The {@link Class}'s {@link ClassLoader} will be used to lookup the Java
     * reflection objects for each individual {@link Class}, {@link Constructor}, {@link Method}, and {@link Field}
     * specified as part of the allowlist in the text file.
     * <p>
     * A single pass is made through each file to collect all the information about each class, constructor, method,
     * and field. Most validation will be done at a later point after all allowlists have been gathered and their
     * merging takes place.
     * <p>
     * A painless type name is one of the following:
     * <ul>
     *     <li> def - The Painless dynamic type which is automatically included without a need to be
     *     allowlisted. </li>
     *     <li> fully-qualified Java type name - Any allowlisted Java class will have the equivalent name as
     *     a Painless type name with the exception that any dollar symbols used as part of inner classes will
     *     be replaced with dot symbols. </li>
     *     <li> short Java type name - The text after the final dot symbol of any specified Java class. A
     *     short type Java name may be excluded by using the 'no_import' attribute during Painless class parsing
     *     as described later. </li>
     * </ul>
     *
     * The following can be parsed from each allowlist text file:
     * <ul>
     *   <li> Blank lines will be ignored by the parser. </li>
     *   <li> Comments may be created starting with a pound '#' symbol and end with a newline. These will
     *   be ignored by the parser. </li>
     *   <li> Primitive types may be specified starting with 'class' and followed by the Java type name,
     *   an opening bracket, a newline, a closing bracket, and a final newline. </li>
     *   <li> Complex types may be specified starting with 'class' and followed by the fully-qualified Java
     *   class name, optionally followed by a 'no_import' attribute, an opening bracket, a newline,
     *   constructor/method/field specifications, a closing bracket, and a final newline. Within a complex
     *   type the following may be parsed:
     *   <ul>
     *     <li> A constructor may be specified starting with an opening parenthesis, followed by a
     *     comma-delimited list of Painless type names corresponding to the type/class names for
     *     the equivalent Java parameter types (these must be allowlisted as well), a closing
     *     parenthesis, and a newline. </li>
     *     <li> A method may be specified starting with a Painless type name for the return type,
     *     followed by the Java name of the method (which will also be the Painless name for the
     *     method), an opening parenthesis, a comma-delimited list of Painless type names
     *     corresponding to the type/class names for the equivalent Java parameter types
     *     (these must be allowlisted as well), a closing parenthesis, and a newline. </li>
     *     <li> An augmented method may be specified starting with a Painless type name for the return
     *     type, followed by the fully qualified Java name of the class the augmented method is
     *     part of (this class does not need to be allowlisted), the Java name of the method
     *     (which will also be the Painless name for the method), an opening parenthesis, a
     *     comma-delimited list of Painless type names corresponding to the type/class names
     *     for the equivalent Java parameter types (these must be allowlisted as well), a closing
     *     parenthesis, and a newline. </li>
     *     <li>A field may be specified starting with a Painless type name for the equivalent Java type
     *     of the field, followed by the Java name of the field (which all be the Painless name
     *     for the field), and a newline. </li>
     *   </ul>
     *   <li> Annotations may be added starting with an at, followed by a name, optionally an opening brace,
     *   a parameter name, an equals, an opening quote, an argument value, a closing quote, (possibly repeated
     *   for multiple arguments,) and a closing brace. Multiple annotations may be added after a class (before
     *   the opening bracket), after a method, or after field. </li>
     * </ul>
     *
     * Note there must be a one-to-one correspondence of Painless type names to Java type/class names.
     * If the same Painless type is defined across multiple files and the Java class is the same, all
     * specified constructors, methods, and fields will be merged into a single Painless type. The
     * Painless dynamic type, 'def', used as part of constructor, method, and field definitions will
     * be appropriately parsed and handled. Painless complex types must be specified with the
     * fully-qualified Java class name. Method argument types, method return types, and field types
     * must be specified with Painless type names (def, fully-qualified, or short) as described earlier.
     * <p>
     * The following example is used to create a single allowlist text file:
     *
     * <pre>
     * # primitive types
     *
     * class int -&gt; int {
     * }
     *
     * # complex types
     *
     * class my.package.Example @no_import {
     *   # constructors
     *   ()
     *   (int)
     *   (def, def)
     *   (Example, def)
     *
     *   # method
     *   Example add(int, def)
     *   int add(Example, Example)
     *   void example() @deprecated[use example 2 instead]
     *   void example2()
     *
     *   # augmented
     *   Example some.other.Class sub(Example, int, def)
     *
     *   # fields
     *   int value0
     *   int value1
     *   def value2
     * }
     * </pre>
     */
    public static Allowlist loadFromResourceFiles(Class<?> resource, Map<String, AllowlistAnnotationParser> parsers, String... filepaths) {
        List<AllowlistClass> allowlistClasses = new ArrayList<>();
        List<AllowlistMethod> allowlistStatics = new ArrayList<>();
        List<AllowlistClassBinding> allowlistClassBindings = new ArrayList<>();

        // Execute a single pass through the allowlist text files. This will gather all the
        // constructors, methods, augmented methods, and fields for each allowlisted class.
        for (String filepath : filepaths) {
            String line;
            int number = -1;

            try (
                LineNumberReader reader = new LineNumberReader(
                    new InputStreamReader(resource.getResourceAsStream(filepath), StandardCharsets.UTF_8)
                )
            ) {

                String parseType = null;
                String allowlistClassOrigin = null;
                String javaClassName = null;
                List<AllowlistConstructor> allowlistConstructors = null;
                List<AllowlistMethod> allowlistMethods = null;
                List<AllowlistField> allowlistFields = null;
                List<Object> classAnnotations = null;

                while ((line = reader.readLine()) != null) {
                    number = reader.getLineNumber();
                    line = line.trim();

                    // Skip any lines that are either blank or comments.
                    if (line.length() == 0 || line.charAt(0) == '#') {
                        continue;
                    }

                    // Handle a new class by resetting all the variables necessary to construct a new AllowlistClass for the allowlist.
                    // Expects the following format: 'class' ID annotations? '{' '\n'
                    if (line.startsWith("class ")) {
                        // Ensure the final token of the line is '{'.
                        if (line.endsWith("{") == false) {
                            throw new IllegalArgumentException(
                                "invalid class definition: failed to parse class opening bracket [" + line + "]"
                            );
                        }

                        if (parseType != null) {
                            throw new IllegalArgumentException("invalid definition: cannot embed class definition [" + line + "]");
                        }

                        // Parse the Java class name and annotations if they exist.
                        int annotationIndex = line.indexOf('@');

                        if (annotationIndex == -1) {
                            annotationIndex = line.length() - 1;
                            classAnnotations = Collections.emptyList();
                        } else {
                            classAnnotations = parseAllowlistAnnotations(parsers, line.substring(annotationIndex, line.length() - 1));
                        }

                        parseType = "class";
                        allowlistClassOrigin = "[" + filepath + "]:[" + number + "]";
                        javaClassName = line.substring(5, annotationIndex).trim();

                        // Reset all the constructors, methods, and fields to support a new class.
                        allowlistConstructors = new ArrayList<>();
                        allowlistMethods = new ArrayList<>();
                        allowlistFields = new ArrayList<>();
                    } else if (line.startsWith("static_import ")) {
                        // Ensure the final token of the line is '{'.
                        if (line.endsWith("{") == false) {
                            throw new IllegalArgumentException(
                                "invalid static import definition: failed to parse static import opening bracket [" + line + "]"
                            );
                        }

                        if (parseType != null) {
                            throw new IllegalArgumentException("invalid definition: cannot embed static import definition [" + line + "]");
                        }

                        parseType = "static_import";

                        // Handle the end of a definition and reset all previously gathered values.
                        // Expects the following format: '}' '\n'
                    } else if (line.equals("}")) {
                        if (parseType == null) {
                            throw new IllegalArgumentException("invalid definition: extraneous closing bracket");
                        }

                        // Create a new AllowlistClass with all the previously gathered constructors, methods,
                        // augmented methods, and fields, and add it to the list of allowlisted classes.
                        if ("class".equals(parseType)) {
                            allowlistClasses.add(
                                new AllowlistClass(
                                    allowlistClassOrigin,
                                    javaClassName,
                                    allowlistConstructors,
                                    allowlistMethods,
                                    allowlistFields,
                                    classAnnotations
                                )
                            );

                            allowlistClassOrigin = null;
                            javaClassName = null;
                            allowlistConstructors = null;
                            allowlistMethods = null;
                            allowlistFields = null;
                            classAnnotations = null;
                        }

                        // Reset the parseType.
                        parseType = null;

                        // Handle static import definition types.
                        // Expects the following format: ID ID '(' ( ID ( ',' ID )* )? ')' ( 'from_class' | 'bound_to' ) ID annotations?
                        // '\n'
                    } else if ("static_import".equals(parseType)) {
                        // Mark the origin of this parsable object.
                        String origin = "[" + filepath + "]:[" + number + "]";

                        // Parse the tokens prior to the method parameters.
                        int parameterStartIndex = line.indexOf('(');

                        if (parameterStartIndex == -1) {
                            throw new IllegalArgumentException(
                                "illegal static import definition: start of method parameters not found [" + line + "]"
                            );
                        }

                        String[] tokens = line.substring(0, parameterStartIndex).trim().split("\\s+");

                        String methodName;

                        // Based on the number of tokens, look up the Java method name.
                        if (tokens.length == 2) {
                            methodName = tokens[1];
                        } else {
                            throw new IllegalArgumentException("invalid method definition: unexpected format [" + line + "]");
                        }

                        String returnCanonicalTypeName = tokens[0];

                        // Parse the method parameters.
                        int parameterEndIndex = line.indexOf(')');

                        if (parameterEndIndex == -1) {
                            throw new IllegalArgumentException(
                                "illegal static import definition: end of method parameters not found [" + line + "]"
                            );
                        }

                        String[] canonicalTypeNameParameters = line.substring(parameterStartIndex + 1, parameterEndIndex)
                            .replaceAll("\\s+", "")
                            .split(",");

                        // Handle the case for a method with no parameters.
                        if ("".equals(canonicalTypeNameParameters[0])) {
                            canonicalTypeNameParameters = new String[0];
                        }

                        // Parse the annotations if they exist.
                        List<Object> annotations;
                        int annotationIndex = line.indexOf('@');

                        if (annotationIndex == -1) {
                            annotationIndex = line.length();
                            annotations = Collections.emptyList();
                        } else {
                            annotations = parseAllowlistAnnotations(parsers, line.substring(annotationIndex));
                        }

                        // Parse the static import type and class.
                        tokens = line.substring(parameterEndIndex + 1, annotationIndex).trim().split("\\s+");

                        String staticImportType;
                        String targetJavaClassName;

                        // Based on the number of tokens, look up the type and class.
                        if (tokens.length == 2) {
                            staticImportType = tokens[0];
                            targetJavaClassName = tokens[1];
                        } else {
                            throw new IllegalArgumentException("invalid static import definition: unexpected format [" + line + "]");
                        }

                        // Add a static import method or binding depending on the static import type.
                        if ("from_class".equals(staticImportType)) {
                            allowlistStatics.add(
                                new AllowlistMethod(
                                    origin,
                                    targetJavaClassName,
                                    methodName,
                                    returnCanonicalTypeName,
                                    Arrays.asList(canonicalTypeNameParameters),
                                    annotations
                                )
                            );
                        } else if ("bound_to".equals(staticImportType)) {
                            allowlistClassBindings.add(
                                new AllowlistClassBinding(
                                    origin,
                                    targetJavaClassName,
                                    methodName,
                                    returnCanonicalTypeName,
                                    Arrays.asList(canonicalTypeNameParameters),
                                    annotations
                                )
                            );
                        } else {
                            throw new IllegalArgumentException(
                                "invalid static import definition: "
                                    + "unexpected static import type ["
                                    + staticImportType
                                    + "] ["
                                    + line
                                    + "]"
                            );
                        }

                        // Handle class definition types.
                    } else if ("class".equals(parseType)) {
                        // Mark the origin of this parsable object.
                        String origin = "[" + filepath + "]:[" + number + "]";

                        // Handle the case for a constructor definition.
                        // Expects the following format: '(' ( ID ( ',' ID )* )? ')' annotations? '\n'
                        if (line.startsWith("(")) {
                            // Parse the constructor parameters.
                            int parameterEndIndex = line.indexOf(')');

                            if (parameterEndIndex == -1) {
                                throw new IllegalArgumentException(
                                    "illegal constructor definition: end of constructor parameters not found [" + line + "]"
                                );
                            }

                            String[] canonicalTypeNameParameters = line.substring(1, parameterEndIndex).replaceAll("\\s+", "").split(",");

                            // Handle the case for a constructor with no parameters.
                            if ("".equals(canonicalTypeNameParameters[0])) {
                                canonicalTypeNameParameters = new String[0];
                            }

                            // Parse the annotations if they exist.
                            List<Object> annotations;
                            int annotationIndex = line.indexOf('@');
                            annotations = annotationIndex == -1
                                ? Collections.emptyList()
                                : parseAllowlistAnnotations(parsers, line.substring(annotationIndex));

                            allowlistConstructors.add(
                                new AllowlistConstructor(origin, Arrays.asList(canonicalTypeNameParameters), annotations)
                            );

                            // Handle the case for a method or augmented method definition.
                            // Expects the following format: ID ID? ID '(' ( ID ( ',' ID )* )? ')' annotations? '\n'
                        } else if (line.contains("(")) {
                            // Parse the tokens prior to the method parameters.
                            int parameterStartIndex = line.indexOf('(');
                            String[] tokens = line.substring(0, parameterStartIndex).trim().split("\\s+");

                            String methodName;
                            String javaAugmentedClassName;

                            // Based on the number of tokens, look up the Java method name and if provided the Java augmented class.
                            if (tokens.length == 2) {
                                methodName = tokens[1];
                                javaAugmentedClassName = null;
                            } else if (tokens.length == 3) {
                                methodName = tokens[2];
                                javaAugmentedClassName = tokens[1];
                            } else {
                                throw new IllegalArgumentException("invalid method definition: unexpected format [" + line + "]");
                            }

                            String returnCanonicalTypeName = tokens[0];

                            // Parse the method parameters.
                            int parameterEndIndex = line.indexOf(')');

                            if (parameterEndIndex == -1) {
                                throw new IllegalArgumentException(
                                    "illegal static import definition: end of method parameters not found [" + line + "]"
                                );
                            }

                            String[] canonicalTypeNameParameters = line.substring(parameterStartIndex + 1, parameterEndIndex)
                                .replaceAll("\\s+", "")
                                .split(",");

                            // Handle the case for a method with no parameters.
                            if ("".equals(canonicalTypeNameParameters[0])) {
                                canonicalTypeNameParameters = new String[0];
                            }

                            // Parse the annotations if they exist.
                            List<Object> annotations;
                            int annotationIndex = line.indexOf('@');
                            annotations = annotationIndex == -1
                                ? Collections.emptyList()
                                : parseAllowlistAnnotations(parsers, line.substring(annotationIndex));

                            allowlistMethods.add(
                                new AllowlistMethod(
                                    origin,
                                    javaAugmentedClassName,
                                    methodName,
                                    returnCanonicalTypeName,
                                    Arrays.asList(canonicalTypeNameParameters),
                                    annotations
                                )
                            );

                            // Handle the case for a field definition.
                            // Expects the following format: ID ID annotations? '\n'
                        } else {
                            // Parse the annotations if they exist.
                            List<Object> annotations;
                            int annotationIndex = line.indexOf('@');

                            if (annotationIndex == -1) {
                                annotationIndex = line.length();
                                annotations = Collections.emptyList();
                            } else {
                                annotations = parseAllowlistAnnotations(parsers, line.substring(annotationIndex));
                            }

                            // Parse the field tokens.
                            String[] tokens = line.substring(0, annotationIndex).split("\\s+");

                            // Ensure the correct number of tokens.
                            if (tokens.length != 2) {
                                throw new IllegalArgumentException("invalid field definition: unexpected format [" + line + "]");
                            }

                            allowlistFields.add(new AllowlistField(origin, tokens[1], tokens[0], annotations));
                        }
                    } else {
                        throw new IllegalArgumentException("invalid definition: unable to parse line [" + line + "]");
                    }
                }

                // Ensure all classes end with a '}' token before the end of the file.
                if (javaClassName != null) {
                    throw new IllegalArgumentException("invalid definition: expected closing bracket");
                }
            } catch (Exception exception) {
                throw new RuntimeException("error in [" + filepath + "] at line [" + number + "]", exception);
            }
        }

        ClassLoader loader = AccessController.doPrivileged((PrivilegedAction<ClassLoader>) resource::getClassLoader);

        return new Allowlist(loader, allowlistClasses, allowlistStatics, allowlistClassBindings, Collections.emptyList());
    }

    private static List<Object> parseAllowlistAnnotations(Map<String, AllowlistAnnotationParser> parsers, String line) {

        List<Object> annotations;

        if ("".equals(line.replaceAll("\\s+", ""))) {
            annotations = Collections.emptyList();
        } else {
            line = line.trim();

            if (line.charAt(0) != '@') {
                throw new IllegalArgumentException("invalid annotation: expected at symbol [" + line + "]");
            }

            if (line.length() < 2) {
                throw new IllegalArgumentException("invalid annotation: expected name [" + line + "]");
            }

            String[] annotationStrings = line.substring(1).split("@");
            annotations = new ArrayList<>(annotationStrings.length);

            for (String annotationString : annotationStrings) {
                String name;
                Map<String, String> arguments;

                annotationString = annotationString.trim();
                int index = annotationString.indexOf('[');

                if (index == -1) {
                    name = annotationString;
                    arguments = Collections.emptyMap();
                } else {
                    if (annotationString.charAt(annotationString.length() - 1) != ']') {
                        throw new IllegalArgumentException("invalid annotation: expected closing brace [" + line + "]");
                    }

                    name = annotationString.substring(0, index);
                    arguments = new HashMap<>();

                    String[] argumentsStrings = annotationString.substring(index + 1, annotationString.length() - 1).split(",");

                    for (String argumentString : argumentsStrings) {
                        String[] argumentKeyValue = argumentString.split("=");

                        if (argumentKeyValue.length != 2) {
                            throw new IllegalArgumentException("invalid annotation: expected key=\"value\" [" + line + "]");
                        }

                        String argumentKey = argumentKeyValue[0].trim();

                        if (argumentKey.isEmpty()) {
                            throw new IllegalArgumentException("invalid annotation: expected key=\"value\" [" + line + "]");
                        }

                        String argumentValue = argumentKeyValue[1];

                        if (argumentValue.length() < 3
                            || argumentValue.charAt(0) != '"'
                            || argumentValue.charAt(argumentValue.length() - 1) != '"') {
                            throw new IllegalArgumentException("invalid annotation: expected key=\"value\" [" + line + "]");
                        }

                        argumentValue = argumentValue.substring(1, argumentValue.length() - 1);

                        arguments.put(argumentKey, argumentValue);
                    }
                }

                AllowlistAnnotationParser parser = parsers.get(name);

                if (parser == null) {
                    throw new IllegalArgumentException("invalid annotation: parser not found for [" + name + "] [" + line + "]");
                }

                annotations.add(parser.parse(arguments));
            }
        }

        return annotations;
    }

    private AllowlistLoader() {}
}
