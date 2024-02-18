/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.gradle.fixtures

import org.gradle.testkit.runner.GradleRunner
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.lang.management.ManagementFactory
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

abstract class AbstractGradleFuncTest extends Specification {

    @Rule
    TemporaryFolder testProjectDir = new TemporaryFolder()

    File settingsFile
    File buildFile

    def setup() {
        settingsFile = testProjectDir.newFile('settings.gradle')
        settingsFile << "rootProject.name = 'hello-world'\n"
        buildFile = testProjectDir.newFile('build.gradle')
    }

    GradleRunner gradleRunner(String... arguments) {
        return gradleRunner(testProjectDir.root, arguments)
    }

    GradleRunner gradleRunner(File projectDir, String... arguments) {
        GradleRunner.create()
                .withDebug(ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0)
                .withProjectDir(projectDir)
                .withArguments(arguments)
                .withPluginClasspath()
                .forwardOutput()
    }

    def assertOutputContains(String givenOutput, String expected) {
        assert normalizedOutput(givenOutput).contains(normalizedOutput(expected))
        true
    }

    String normalizedOutput(String input) {
        String normalizedPathPrefix = testProjectDir.root.canonicalPath.replace('\\', '/')
        return input.readLines()
                .collect { it.replace('\\', '/') }
                .collect {it.replace(normalizedPathPrefix , '.') }
                .join("\n")
    }

    File file(String path) {
        File newFile = new File(testProjectDir.root, path)
        newFile.getParentFile().mkdirs()
        newFile
    }

    File someJar(String fileName = 'some.jar') {
        File jarFolder = new File(testProjectDir.root, "jars");
        jarFolder.mkdirs()
        File jarFile = new File(jarFolder, fileName)
        JarEntry entry = new JarEntry("foo.txt");

        jarFile.withOutputStream {
            JarOutputStream target = new JarOutputStream(it)
            target.putNextEntry(entry);
            target.closeEntry();
            target.close();
        }

        return jarFile;
    }

    File internalBuild(File buildScript = buildFile) {
        buildScript << """plugins {
          id 'opensearch.global-build-info'
        }
        import org.opensearch.gradle.Architecture
        import org.opensearch.gradle.info.BuildParams

        BuildParams.init { it.setIsInternal(true) }

        import org.opensearch.gradle.BwcVersions
        import org.opensearch.gradle.Version

        Version currentVersion = Version.fromString("9.0.0")
        BwcVersions versions = new BwcVersions(new TreeSet<>(
        Arrays.asList(Version.fromString("8.0.0"), Version.fromString("8.0.1"), Version.fromString("8.1.0"), currentVersion)),
            currentVersion)

        BuildParams.init { it.setBwcVersions(versions) }
        """
    }

    void setupLocalGitRepo() {
        //TODO: cleanup
        execute("git init")
        execute('git config user.email "build-tool@opensearch.org"')
        execute('git config user.name "Build tool"')
        execute("git add .")
        execute('git commit -m "Initial"')
    }

    void execute(String command, File workingDir = testProjectDir.root) {
        def proc = command.execute(Collections.emptyList(), workingDir)
        proc.waitFor()
        if(proc.exitValue()) {
            println "Error running command ${command}:"
            println "Syserr: " + proc.errorStream.text
        }
    }
}
