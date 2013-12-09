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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Pattern;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldRole;
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
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.converters.TypeConverter;
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
    Stack<MongoExpression> stack = new Stack();

    public QueryToMongoDBMapper(QueryCompilation compilation, Map parameters, AbstractClassMetaData cmd,
            ExecutionContext ec, Query q)
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
        compileFrom();
        compileFilter();
        compileResult();
        compileGrouping();
        compileHaving();
        compileOrdering();
    }

    /**
     * Method to compile the FROM clause of the query
     */
    protected void compileFrom()
    {
        if (compilation.getExprFrom() != null)
        {
            // Process all ClassExpression(s) in the FROM, adding joins to the statement as required
            compileComponent = CompilationComponent.FROM;
            Expression[] fromExprs = compilation.getExprFrom();
            for (int i=0;i<fromExprs.length;i++)
            {
                // TODO Compile FROM class expression
            }
        }
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
            for (Expression expr :  resultExprs)
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
     * Method to compile the grouping clause of the query
     */
    protected void compileGrouping()
    {
        if (compilation.getExprGrouping() != null)
        {
            // Apply any grouping to the statement
            compileComponent = CompilationComponent.GROUPING;
            Expression[] groupExprs = compilation.getExprGrouping();
            for (int i = 0; i < groupExprs.length; i++)
            {
                // TODO Compile grouping
            }
            compileComponent = null;
        }
    }

    /**
     * Method to compile the having clause of the query
     */
    protected void compileHaving()
    {
        if (compilation.getExprHaving() != null)
        {
            // Apply any having to the statement
            compileComponent = CompilationComponent.HAVING;
            /*Expression havingExpr = */compilation.getExprHaving();
            // TODO Compile having
            compileComponent = null;
        }
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

    /* (non-Javadoc)
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

    /* (non-Javadoc)
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

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processEqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processEqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)right, (MongoLiteral)left, MongoOperator.OP_EQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)left, (MongoLiteral)right, MongoOperator.OP_EQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processEqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNoteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)right, (MongoLiteral)left, MongoOperator.OP_NOTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)left, (MongoLiteral)right, MongoOperator.OP_NOTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processNoteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)right, (MongoLiteral)left, MongoOperator.OP_LTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)left, (MongoLiteral)right, MongoOperator.OP_GT);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processGtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)right, (MongoLiteral)left, MongoOperator.OP_GTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)left, (MongoLiteral)right, MongoOperator.OP_LT);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processLtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)right, (MongoLiteral)left, MongoOperator.OP_LT);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)left, (MongoLiteral)right, MongoOperator.OP_GTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processGteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof MongoLiteral && right instanceof MongoFieldExpression)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)right, (MongoLiteral)left, MongoOperator.OP_GT);
            stack.push(mongoExpr);
            return mongoExpr;
        }
        else if (left instanceof MongoFieldExpression && right instanceof MongoLiteral)
        {
            MongoExpression mongoExpr =
                new MongoBooleanExpression((MongoFieldExpression)left, (MongoLiteral)right, MongoOperator.OP_LTEQ);
            stack.push(mongoExpr);
            return mongoExpr;
        }

        // TODO Auto-generated method stub
        return super.processLteqExpression(expr);
    }
    
    /* (non-Javadoc)
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
                NucleusLogger.QUERY.debug(">> Primary " + expr +
                    " is not stored in this document, so unexecutable in datastore");
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

    /* (non-Javadoc)
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
            else if (parameters != null && parameters.containsKey(expr.getId()))
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
                    positionalParamNumber = position+1;
                }
            }
        }

        // TODO Change this to use MongoDBUtils.getStoredValueForField
        if (paramValueSet)
        {
            if (paramValue instanceof Number)
            {
                MongoLiteral lit = new MongoLiteral(paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof String)
            {
                MongoLiteral lit = new MongoLiteral(paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof Character)
            {
                MongoLiteral lit = new MongoLiteral("" + paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof Boolean)
            {
                MongoLiteral lit = new MongoLiteral(paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof Enum)
            {
                MongoLiteral lit = new MongoLiteral(paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof java.sql.Time || paramValue instanceof java.sql.Date)
            {
                // java.sql.Time/Date are stored via converter
                Object storedVal = paramValue;
                Class paramType = paramValue.getClass();
                if (paramValue instanceof SCO)
                {
                    paramType = ((SCO)paramValue).getValue().getClass();
                }
                TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(paramType, String.class);
                TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(paramType, Long.class);
                if (strConv != null)
                {
                    // store as a String
                    storedVal = strConv.toDatastoreType(paramValue);
                }
                else if (longConv != null)
                {
                    // store as a Long
                    storedVal = longConv.toDatastoreType(paramValue);
                }
                MongoLiteral lit = new MongoLiteral(storedVal);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof java.util.Calendar)
            {
                Object storedVal = MongoDBUtils.getStoredValueForField(ec, null, paramValue, FieldRole.ROLE_FIELD);
                MongoLiteral lit = new MongoLiteral(storedVal);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof Date)
            {
                MongoLiteral lit = new MongoLiteral(paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else if (paramValue instanceof Collection)
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
            else if (paramValue == null)
            {
                MongoLiteral lit = new MongoLiteral(paramValue);
                stack.push(lit);
                precompilable = false;
                return lit;
            }
            else
            {
                NucleusLogger.QUERY.info("Dont currently support parameter values of type " + paramValue.getClass().getName());
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

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLiteral(org.datanucleus.query.expression.Literal)
     */
    @Override
    protected Object processLiteral(Literal expr)
    {
        Object litValue = expr.getLiteral();
        if (litValue instanceof BigDecimal)
        {
            // MongoDB can't cope with BigDecimal, so give it a Double
            MongoLiteral lit = new MongoLiteral(((BigDecimal)litValue).doubleValue());
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
            MongoLiteral lit = new MongoLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        // TODO Handle all MongoDB supported (literal) types

        return super.processLiteral(expr);
    }

    /* (non-Javadoc)
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
        MongoExpression mongoExprArg0 = (mongoExprArgs.size() == 1) ? mongoExprArgs.get(0) : null;
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
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, invokedExprArg, MongoOperator.REGEX);
                    }
                    else if ("startsWith".equals(operation))
                    {
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, new MongoLiteral("^" + Pattern.quote(invokedExprArg.getValue()
                            .toString())), MongoOperator.REGEX);
                    }
                    else if ("endsWith".equals(operation))
                    {
                        mongoExpr = new MongoBooleanExpression(invokedFieldExpr, new MongoLiteral(Pattern.quote(invokedExprArg.getValue()
                            .toString()) + "$"), MongoOperator.REGEX);
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

        NucleusLogger.QUERY.debug(">> Dont currently support method invocation in MongoDB datastore queries : method=" + operation + 
            " args=" + StringUtils.collectionToString(args));
        return super.processInExpression(invokedExpr);
    }

    /**
     * Convenience method to return the "field name" in candidate document for this primary.
     * Allows for simple relation fields, and (nested) embedded PC fields - i.e all fields that are present
     * in the document.
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
        AbstractMemberMetaData embMmd = null;
        boolean embeddedFlat = false;
        String embeddedNestedField = null;

        boolean firstTuple = true;
        Iterator<String> iter = tuples.iterator();
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
                RelationType relationType = mmd.getRelationType(ec.getClassLoaderResolver());
                if (relationType == RelationType.NONE)
                {
                    if (iter.hasNext())
                    {
                        throw new NucleusUserException("Query has reference to " +
                            StringUtils.collectionToString(tuples) + " yet " + name + " is a non-relation field!");
                    }
                    if (embMmd != null)
                    {
                        if (embeddedFlat)
                        {
                            return new MongoFieldExpression(
                                MongoDBUtils.getFieldName(embMmd, mmd.getAbsoluteFieldNumber()), mmd);
                        } 
                        else
                        {
                            return new MongoFieldExpression(
                                embeddedNestedField + "." + ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN), mmd);
                        }
                    }
                    return new MongoFieldExpression(
                        ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN), mmd);
                }
                else
                {
                    boolean embedded = mmd.isEmbedded();
                    if (!embedded)
                    {
                        // Not explicitly marked as embedded but check whether it is defined in JDO embedded metadata
                        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
                        if (embmd == null && embMmd != null)
                        {
                            embmd = embMmd.getEmbeddedMetaData();
                        }
                        if (embmd != null)
                        {
                            AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
                            if (embmmds != null)
                            {
                                for (int i=0;i<embmmds.length;i++)
                                {
                                    if (embmmds[i].getName().equals(mmd.getName()))
                                    {
                                        embedded = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if (embedded)
                    {
                        if (RelationType.isRelationSingleValued(relationType))
                        {
                            boolean nested = true;
                            String nestedStr = mmd.getValueForExtension("nested");
                            if (nestedStr != null && nestedStr.equalsIgnoreCase("false"))
                            {
                                nested = false;
                            }

                            cmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), ec.getClassLoaderResolver());
                            embMmd = mmd;
                            if (nested)
                            {
                                if (embeddedNestedField == null)
                                {
                                    embeddedNestedField = ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
                                }
                                else
                                {
                                    embeddedNestedField += ("." + ec.getStoreManager().getNamingFactory().getColumnName(mmd, ColumnType.COLUMN));
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
                        else if (RelationType.isRelationMultiValued(relationType))
                        {
                            throw new NucleusUserException("Dont currently support querying of embedded collection fields at " + mmd.getFullFieldName());
                        }
                    }
                    else
                    {
                        // Not embedded
                        // TODO Understand this logic (was included in some patch for NUCMONGODB-65)
                        // Makes little sense to me - the only thing I would understand is if iter.hasNext() is true
                        // then abort since we can't join to some other object
                        if (relationType == RelationType.ONE_TO_MANY_UNI || relationType == RelationType.ONE_TO_MANY_BI ||
                            relationType == RelationType.MANY_TO_ONE_UNI || relationType == RelationType.MANY_TO_ONE_BI)
                        {
                            if (mmd.getMappedBy() != null)
                            {
                                // FK on the other side -- requires a join, not natively supported by mongo
                                throw new NucleusException("Querying of relationships from the non-owning side not currently supported (for name: " + name + " in " + StringUtils.collectionToString(tuples));
                            } 
                            else
                            {
                                // FK is at this side, so only join if further component provided, or if forcing...
                                if (iter.hasNext())
                                {
                                    // ... native joins not supported by mongo, so bail
                                    throw new NucleusException("Querying of joined attributes not supported by data store (for name: " + name + " in " + StringUtils.collectionToString(tuples));
                                } 
                                else
                                {
                                    if (expr.getParent() != null && expr.getParent().getOperator() == Expression.OP_CAST)
                                    {
                                        throw new NucleusException("Cast not supported (for name: " + name + " in " + StringUtils.collectionToString(tuples));
                                    }
                                    return new MongoFieldExpression(name, mmd);
                                }
                            }
                        }
                        else
                        {
                            if (compileComponent == CompilationComponent.FILTER)
                            {
                                filterComplete = false;
                            } 
                            else
                            {
                                if (compileComponent == CompilationComponent.RESULT)
                                {
                                    resultComplete = false;
                                }
                            }

                            NucleusLogger.QUERY.debug("Query has reference to " + 
                                    StringUtils.collectionToString(tuples) + " and " + mmd.getFullFieldName() +
                                    " is not persisted into this document, so unexecutable in the datastore");
                            return null;
                        }
                    }
                }
                firstTuple = false;
            }
        }

        return null;
    }
}
