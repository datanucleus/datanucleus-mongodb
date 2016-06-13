/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
   ...
**********************************************************************/
package org.datanucleus.store.mongodb.query;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.InvokeExpression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.store.mongodb.MongoDBUtils;
import org.datanucleus.store.mongodb.query.expression.MongoBooleanExpression;
import org.datanucleus.store.mongodb.query.expression.MongoExpression;
import org.datanucleus.store.mongodb.query.expression.MongoFieldExpression;
import org.datanucleus.store.mongodb.query.expression.MongoLiteral;
import org.datanucleus.store.mongodb.query.expression.MongoOperator;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.mongodb.BasicDBObject;

/**
 * Class which maps a compiled (generic) query to an MongoDB query.
 */
public class QueryToMongoDBMapper extends AbstractExpressionEvaluator
{
    final ExecutionContext ec;

    final String candidateAlias;

    final AbstractClassMetaData candidateCmd;

    final Query query;

    final QueryCompilation compilation;

    /** Input parameter values, keyed by the parameter name. Will be null if compiled pre-execution. */
    final Map parameters;

    /** Positional parameter that we are up to (-1 implies not being used). */
    int positionalParamNumber = -1;

    /** State variable for the component being compiled. */
    CompilationComponent compileComponent;

    /** Whether the filter clause is completely evaluatable in the datastore. */
    boolean filterComplete = true;

    /** The filter expression. If no filter is required then this will be null. */
    MongoBooleanExpression filterExpr;

    /** The ordering object. If no ordering is requested then this will be null. */
    BasicDBObject orderingObject;

    /** Whether the result clause is completely evaluatable in the datastore. */
    boolean resultComplete = true;

    /** The result object. If no result is required then this will be null. */
    MongoDBResult resultObject;

    boolean precompilable = true;

    /** Stack of mongo expressions, used for compilation of the query into MongoDB objects. */
    Deque<MongoExpression> stack = new ArrayDeque<MongoExpression>();

    public QueryToMongoDBMapper(QueryCompilation compilation, Map parameters, AbstractClassMetaData cmd, ExecutionContext ec, Query q)
    {
        this.ec = ec;
        this.query = q;
        this.compilation = compilation;
        this.parameters = parameters;
        this.candidateCmd = cmd;
        this.candidateAlias = compilation.getCandidateAlias();
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public MongoBooleanExpression getFilterExpression()
    {
        return filterExpr;
    }

    public MongoDBResult getResultObject()
    {
        return resultObject;
    }

    public BasicDBObject getOrderingObject()
    {
        return orderingObject;
    }

    public void compile()
    {
        if (compilation.getExprFrom() != null)
        {
            Expression[] fromExpr = compilation.getExprFrom();
            if (fromExpr != null)
            {
                if (fromExpr.length > 1)
                {
                    NucleusLogger.QUERY.warn("FROM clause will be ignored. Not supported for this datastore (MongoDB doesn't do 'joins')");
                }
                else if (fromExpr.length == 1)
                {
                    if (fromExpr[0].getRight() != null)
                    {
                        NucleusLogger.QUERY.warn("FROM clause will be ignored. Not supported for this datastore (MongoDB doesn't do 'joins')");
                    }
                }
            }
        }
        compileFilter();
        compileResult();
        if (compilation.getExprGrouping() != null)
        {
            NucleusLogger.QUERY.warn("GROUPING clause will be ignored. Not supported for this datastore");
        }
        if (compilation.getExprHaving() != null)
        {
            NucleusLogger.QUERY.warn("HAVING clause will be ignored. Not supported for this datastore");
        }
        compileOrdering();
    }

    /**
     * Method to compile the WHERE clause of the query
     */
    protected void compileFilter()
    {
        if (compilation.getExprFilter() != null)
        {
            compileComponent = CompilationComponent.FILTER;

            try
            {
                compilation.getExprFilter().evaluate(this);
                MongoExpression mongoExpr = stack.pop();
                if (!(mongoExpr instanceof MongoBooleanExpression))
                {
                    NucleusLogger.QUERY.error("Invalid compilation : filter compiled to " + mongoExpr);
                    filterComplete = false;
                }
                else
                {
                    filterExpr = (MongoBooleanExpression) mongoExpr;
                }
            }
            catch (Exception e)
            {
                // Impossible to compile all to run in the datastore, so just exit
                if (NucleusLogger.QUERY.isDebugEnabled())
                {
                    NucleusLogger.QUERY.debug("Compilation of filter to be evaluated completely in-datastore was impossible : " + e.getMessage());
                }
                filterComplete = false;
            }

            compileComponent = null;
        }
    }

    /**
     * Method to compile the result clause of the query
     */
    protected void compileResult()
    {
        if (compilation.getExprResult() != null)
        {
            compileComponent = CompilationComponent.RESULT;
            resultObject = new MongoDBResult();

            // Select any result expressions
            Expression[] resultExprs = compilation.getExprResult();
            for (Expression expr : resultExprs)
            {
                if (expr instanceof InvokeExpression)
                {
                    if ("count".equalsIgnoreCase(((InvokeExpression) expr).getOperation()) && resultExprs.length == 1)
                    {
                        resultObject.setCountOnly(true);
                    }
                    else
                    {

                    }
                }
                else
                {

                }
            }
        }
        // TODO Handle distinct
        compileComponent = null;
    }

    /**
     * Method to compile the ordering clause of the query
     */
    protected void compileOrdering()
    {
        if (compilation.getExprOrdering() != null)
        {
            compileComponent = CompilationComponent.ORDERING;
            Expression[] orderingExpr = compilation.getExprOrdering();
            orderingObject = new BasicDBObject();
            for (int i = 0; i < orderingExpr.length; i++)
            {
                OrderExpression orderExpr = (OrderExpression) orderingExpr[i];
                MongoFieldExpression orderMongoExpr = (MongoFieldExpression) orderExpr.getLeft().evaluate(this);
                String orderDir = orderExpr.getSortOrder();
                int direction = ((orderDir == null || orderDir.equals("ascending")) ? 1 : -1);
                orderingObject.put(orderMongoExpr.getPropertyName(), direction);
            }
            compileComponent = null;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processAndExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processAndExpression(Expression expr)
    {
        MongoBooleanExpression right = (MongoBooleanExpression) stack.pop();
        MongoBooleanExpression left = (MongoBooleanExpression) stack.pop();
        MongoBooleanExpression andExpr = new MongoBooleanExpression(left, right, MongoOperator.OP_AND);
        stack.push(andExpr);
        return andExpr;
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processOrExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processOrExpression(Expression expr)
    {
        MongoBooleanExpression right = (MongoBooleanExpression) stack.pop();
        MongoBooleanExpression left = (MongoBooleanExpression) stack.pop();
        MongoBooleanExpression andExpr = new MongoBooleanExpression(left, right, MongoOperator.OP_OR);
        stack.push(andExpr);
        return andExpr;
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processEqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processEqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) right, (MongoLiteral) left, MongoOperator.OP_EQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) left, (MongoLiteral) right, MongoOperator.OP_EQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processEqExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNoteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) right, (MongoLiteral) left, MongoOperator.OP_NOTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) left, (MongoLiteral) right, MongoOperator.OP_NOTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processNoteqExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) right, (MongoLiteral) left, MongoOperator.OP_LTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) left, (MongoLiteral) right, MongoOperator.OP_GT);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processGtExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) right, (MongoLiteral) left, MongoOperator.OP_GTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) left, (MongoLiteral) right, MongoOperator.OP_LT);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processLtExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) right, (MongoLiteral) left, MongoOperator.OP_LT);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) left, (MongoLiteral) right, MongoOperator.OP_GTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processGteqExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) right, (MongoLiteral) left, MongoOperator.OP_GT);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr = new MongoBooleanExpression((MongoFieldExpression) left, (MongoLiteral) right, MongoOperator.OP_LTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processLteqExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processPrimaryExpression(org.datanucleus.query.expression.PrimaryExpression)
     */
    @Override
    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
        Expression left = expr.getLeft();
        if (left == null)
        {
            MongoFieldExpression fieldExpr = getFieldNameForPrimary(expr);
            if (fieldExpr == null)
            {
                if (compileComponent == CompilationComponent.FILTER)
                {
                    filterComplete = false;
                }
                else if (compileComponent == CompilationComponent.RESULT)
                {
                    resultComplete = false;
                }
                NucleusLogger.QUERY.warn("Primary " + expr + " is not stored in this document, so unexecutable in datastore");
            }
            else
            {
                stack.push(fieldExpr);
                return fieldExpr;
            }
        }

        // TODO Auto-generated method stub
        return super.processPrimaryExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processParameterExpression(org.datanucleus.query.expression.ParameterExpression)
     */
    @Override
    protected Object processParameterExpression(ParameterExpression expr)
    {
        // Extract the parameter value (if set)
        Object paramValue = null;
        boolean paramValueSet = false;
        if (parameters != null && !parameters.isEmpty())
        {
            // Check if the parameter has a value
            if (parameters.containsKey(expr.getId()))
            {
                // Named parameter
                paramValue = parameters.get(expr.getId());
                paramValueSet = true;
            }
            else if (parameters.containsKey(expr.getId()))
            {
                // Positional parameter, but already encountered
                paramValue = parameters.get(expr.getId());
                paramValueSet = true;
            }
            else
            {
                // Positional parameter, not yet encountered
                int position = positionalParamNumber;
                if (positionalParamNumber < 0)
                {
                    position = 0;
                }
                if (parameters.containsKey(Integer.valueOf(position)))
                {
                    paramValue = parameters.get(Integer.valueOf(position));
                    paramValueSet = true;
                    positionalParamNumber = position + 1;
                }
            }
        }

        // TODO Change this to use MongoDBUtils.getStoredValueForField
        if (paramValueSet)
        {
            if (paramValue == null)
            {
                MongoLiteral lit = new MongoLiteral(null);
                stack.push(lit);
                precompilable = false;
                return lit;
            }

            // Make sure we only use a type that MongoDB accepts
            paramValue = MongoDBUtils.getAcceptableDatastoreValue(paramValue);

            if (paramValue instanceof Number || paramValue instanceof String || paramValue instanceof Character || paramValue instanceof Boolean || paramValue instanceof Enum ||
                    paramValue instanceof Date || paramValue instanceof Collection || paramValue instanceof java.util.Calendar)
            {
                MongoLiteral lit = new MongoLiteral(paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (ec.getApiAdapter().isPersistable(paramValue))
            {
                MongoLiteral lit = new MongoLiteral(String.valueOf(ec.getApiAdapter().getIdForObject(paramValue)));
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else
            {
                NucleusLogger.QUERY.warn("Dont currently support parameter values of type " + paramValue.getClass().getName());
                // TODO Support other parameter value types
            }
        }
        else
        {
            precompilable = false;
            throw new NucleusException("Parameter " + expr + " is not currently set, so cannot complete the compilation");
        }

        return super.processParameterExpression(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLiteral(org.datanucleus.query.expression.Literal)
     */
    @Override
    protected Object processLiteral(Literal expr)
    {
        Object litValue = expr.getLiteral();
        if (litValue instanceof BigDecimal)
        {
            // MongoDB can't cope with BigDecimal, so give it a Double
            MongoLiteral lit = new MongoLiteral(((BigDecimal) litValue).doubleValue());
            stack.push(lit);
            return lit;
        }
        else if (litValue instanceof Number)
        {
            MongoLiteral lit = new MongoLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        else if (litValue instanceof String)
        {
            MongoLiteral lit = new MongoLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        else if (litValue instanceof Character)
        {
            MongoLiteral lit = new MongoLiteral("" + litValue);
            stack.push(lit);
            return lit;
        }
        else if (litValue instanceof Boolean)
        {
            MongoLiteral lit = new MongoLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        else if (litValue == null)
        {
            MongoLiteral lit = new MongoLiteral(null);
            stack.push(lit);
            return lit;
        }
        // TODO Handle all MongoDB supported (literal) types

        return super.processLiteral(expr);
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processInvokeExpression(org.datanucleus.query.expression.InvokeExpression)
     */
    @Override
    protected Object processInvokeExpression(InvokeExpression expr)
    {
        boolean supported = true;

        // Find object that we invoke on
        Expression invokedExpr = expr.getLeft();
        String operation = expr.getOperation();
        List<Expression> args = expr.getArguments();

        MongoExpression invokedMongoExpr = null;
        if (invokedExpr == null)
        {
            // Static method
        }
        else if (invokedExpr instanceof PrimaryExpression)
        {
            processPrimaryExpression((PrimaryExpression) invokedExpr);
            invokedMongoExpr = stack.pop();
        }
        else if (invokedExpr instanceof ParameterExpression)
        {
            processParameterExpression((ParameterExpression) invokedExpr);
            invokedMongoExpr = stack.pop();
        }
        else
        {
            supported = false;
        }

        List<MongoExpression> mongoExprArgs = null;
        if (supported && args != null)
        {
            mongoExprArgs = new ArrayList<MongoExpression>();
            for (Expression argExpr : args)
            {

                if (argExpr instanceof PrimaryExpression)
                {
                    processPrimaryExpression((PrimaryExpression) argExpr);
                    mongoExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof ParameterExpression)
                {
                    processParameterExpression((ParameterExpression) argExpr);
                    mongoExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof InvokeExpression)
                {
                    processInvokeExpression((InvokeExpression) argExpr);
                    mongoExprArgs.add(stack.pop());
                }
                else if (argExpr instanceof Literal)
                {
                    processLiteral((Literal) argExpr);
                    mongoExprArgs.add(stack.pop());
                }
                else
                {
                    supported = false;
                    break;
                }
            }
        }

        MongoExpression mongoExpr = null;
        MongoExpression mongoExprArg0 = (mongoExprArgs != null && mongoExprArgs.size() == 1) ? mongoExprArgs.get(0) : null;
        if (supported)
        {
            if (invokedMongoExpr instanceof MongoFieldExpression && mongoExprArg0 instanceof MongoLiteral)
            {
                MongoFieldExpression invokedFieldExpr = (MongoFieldExpression) invokedMongoExpr;
                MongoLiteral invokedExprArg = (MongoLiteral) mongoExprArg0;

                if (invokedFieldExpr.getMemberMetaData().getType() == String.class)
                {
                    // String methods
                    if ("equals".equals(operation))
                    {
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, invokedExprArg, MongoOperator.OP_EQ);
                    }
                    else if ("matches".equals(operation))
                    {
                        // TODO Need to check the pattern and map it on to MongoDB REGEX patterns
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, invokedExprArg, MongoOperator.REGEX);
                    }
                    else if ("startsWith".equals(operation))
                    {
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, new MongoLiteral("^" + Pattern.quote(invokedExprArg.getValue().toString())), MongoOperator.REGEX);
                    }
                    else if ("endsWith".equals(operation))
                    {
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, new MongoLiteral(Pattern.quote(invokedExprArg.getValue().toString()) + "$"), MongoOperator.REGEX);
                    }
                }
                else if (invokedFieldExpr.getMemberMetaData().hasCollection())
                {
                    // Collections methods
                    if ("contains".equals(operation))
                    {
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, invokedExprArg, MongoOperator.OP_EQ);
                    }
                }
            }
            else if (invokedMongoExpr instanceof MongoLiteral && mongoExprArg0 instanceof MongoFieldExpression)
            {
                MongoLiteral invokedLiteralExpr = (MongoLiteral) invokedMongoExpr;
                MongoFieldExpression invokedExprArg = (MongoFieldExpression) mongoExprArg0;

                if (invokedLiteralExpr.getValue() instanceof Collection)
                {
                    // Collection methods
                    if ("contains".equals(operation))
                    {
                        mongoExpr = new MongoBooleanExpression(invokedExprArg, invokedLiteralExpr, MongoOperator.IN);
                    }
                }
            }
        }
        if (mongoExpr != null)
        {
            stack.push(mongoExpr);
            return mongoExpr;
        }

        NucleusLogger.QUERY.warn("Dont currently support method invocation in MongoDB datastore queries : method=" + operation + " args=" + StringUtils.collectionToString(args));
        return super.processInvokeExpression(expr);
    }

    /**
     * Convenience method to return the "field name" in candidate document for this primary. Allows for simple
     * relation fields, and (nested) embedded PC fields - i.e all fields that are present in the document.
     * @param expr The expression
     * @return The document field name for this primary (or null if not resolvable in this document)
     */
    protected MongoFieldExpression getFieldNameForPrimary(PrimaryExpression expr)
    {
        List<String> tuples = expr.getTuples();
        if (tuples == null || tuples.isEmpty())
        {
            return null;
        }

        AbstractClassMetaData cmd = candidateCmd;
        Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
        AbstractMemberMetaData embMmd = null;
        boolean embeddedFlat = false;
        String embeddedNestedField = null;

        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
        boolean firstTuple = true;
        Iterator<String> iter = tuples.iterator();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        while (iter.hasNext())
        {
            String name = iter.next();
            if (firstTuple && name.equals(candidateAlias))
            {
                cmd = candidateCmd;
            }
            else
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(name);
                RelationType relationType = mmd.getRelationType(clr);
                if (relationType == RelationType.NONE)
                {
                    if (iter.hasNext())
                    {
                        throw new NucleusUserException("Query has reference to " + StringUtils.collectionToString(tuples) + " yet " + name + " is a non-relation field!");
                    }

                    if (embMmd != null)
                    {
                        if (embeddedFlat)
                        {
                            embMmds.add(mmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(embMmds);
                            return new MongoFieldExpression(mapping.getColumn(0).getName(), mmd, mapping);
                        }

                        embMmds.add(mmd);
                        MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(embMmds);
                        return new MongoFieldExpression(embeddedNestedField + "." + table.getMemberColumnMappingForEmbeddedMember(embMmds).getColumn(0).getName(), mmd, mapping);
                    }
                    MemberColumnMapping mapping = table.getMemberColumnMappingForMember(mmd);
                    return new MongoFieldExpression(mapping.getColumn(0).getName(), mmd, mapping);
                }
                else if (RelationType.isRelationSingleValued(relationType))
                {
                    boolean embedded = MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, embMmds.isEmpty() ? null : embMmds.get(embMmds.size() - 1));

                    if (embedded)
                    {
                        boolean nested = MongoDBUtils.isMemberNested(mmd);

                        cmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), ec.getClassLoaderResolver());
                        embMmd = mmd;
                        embMmds.add(embMmd);
                        if (nested)
                        {
                            if (embeddedNestedField == null)
                            {
                                embeddedNestedField = table.getMemberColumnMappingForMember(mmd).getColumn(0).getName();
                            }
                            else
                            {
                                embeddedNestedField += ("." + table.getMemberColumnMappingForEmbeddedMember(embMmds).getColumn(0).getName());
                            }
                        }
                        else
                        {
                            // Embedded PC, with fields flat in the document of the owner
                            if (!embeddedFlat)
                            {
                                embeddedFlat = true;
                            }
                        }
                    }
                    else
                    {
                        // 1-1/N-1 not embedded
                        if (embMmds.isEmpty() && !iter.hasNext())
                        {
                            // Last tuple, and not part of an embedded chain
                            MemberColumnMapping mapping = table.getMemberColumnMappingForMember(mmd);
                            String fieldName = mapping.getColumn(0).getName();
                            return new MongoFieldExpression(fieldName, mmd, mapping);
                        }

                        // Either a 1-1 with further components in the chain, or a 1-1 after an embedded. NOT SUPPORTED
                        embMmds.clear();

                        if (compileComponent == CompilationComponent.FILTER)
                        {
                            filterComplete = false;
                        }
                        else if (compileComponent == CompilationComponent.RESULT)
                        {
                            resultComplete = false;
                        }

                        NucleusLogger.QUERY.warn("Query has reference to " + StringUtils.collectionToString(tuples) + " and " + mmd.getFullFieldName() + 
                                " is not persisted into this document, so unexecutable in the datastore");
                        return null;
                    }
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    throw new NucleusUserException("Dont currently support querying of multi-valued fields at " + mmd.getFullFieldName());
                }

                firstTuple = false;
            }
        }

        return null;
    }
}