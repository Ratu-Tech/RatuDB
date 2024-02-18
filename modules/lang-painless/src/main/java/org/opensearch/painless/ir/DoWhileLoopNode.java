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

package org.opensearch.painless.ir;

import org.opensearch.painless.ClassWriter;
import org.opensearch.painless.Location;
import org.opensearch.painless.MethodWriter;
import org.opensearch.painless.phase.IRTreeVisitor;
import org.opensearch.painless.symbol.WriteScope;
import org.opensearch.painless.symbol.WriteScope.Variable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

public class DoWhileLoopNode extends LoopNode {

    /* ---- begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitDoWhileLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getBlockNode().visit(irTreeVisitor, scope);

        if (getConditionNode() != null) {
            getConditionNode().visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public DoWhileLoopNode(Location location) {
        super(location);
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeStatementOffset(getLocation());

        writeScope = writeScope.newScope();

        Label start = new Label();
        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(start);

        getBlockNode().continueLabel = begin;
        getBlockNode().breakLabel = end;
        getBlockNode().write(classWriter, methodWriter, writeScope);

        methodWriter.mark(begin);

        if (isContinuous() == false) {
            getConditionNode().write(classWriter, methodWriter, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), getLocation());
        }

        methodWriter.goTo(start);
        methodWriter.mark(end);
    }
}
