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
package org.datanucleus.store.mongodb.query.expression;

import com.mongodb.BasicDBObject;

import java.util.ArrayList;
import java.util.Collection;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;

/**
 * Representation of a boolean expression in MongoDB queries.
 * Has the DBObject that it represents.
 */
public class MongoBooleanExpression extends MongoExpression
{
    BasicDBObject dbObject = null;

    /**
     * Constructor when the expression represents a comparison, between the field expression and a literal.
     * @param fieldExpr Field Expression
     * @param lit The literal
     * @param op The operator (eq, noteq, lt, gt, etc)
     */
    public MongoBooleanExpression(MongoFieldExpression fieldExpr, MongoLiteral lit, MongoOperator op)
    {
        String propName = fieldExpr.getPropertyName();
        Object value = lit.getValue();
        if (value instanceof Enum)
        {
            value = asEnumValue(fieldExpr, (Enum<?>) value);
        }
        else if (value instanceof Collection)
        {
            Collection<Object> collection = new ArrayList<Object>();
            for (Object obj : (Collection<?>) value)
            {
                if (obj instanceof Enum) {
                    collection.add(asEnumValue(fieldExpr, (Enum<?>) obj));
                } else {
                    collection.add(obj);
                }
            }
            value = collection;
        }

        if (op == MongoOperator.OP_EQ)
        {
            dbObject = new BasicDBObject(propName, value);
        }
        else if (op == MongoOperator.OP_NOTEQ || op == MongoOperator.OP_LT || op == MongoOperator.OP_LTEQ ||
            op == MongoOperator.OP_GT || op == MongoOperator.OP_GTEQ || op == MongoOperator.REGEX || op == MongoOperator.IN)
        {
            BasicDBObject valObject = new BasicDBObject(op.getValue(), value);
            dbObject = new BasicDBObject(propName, valObject);
        }
        else
        {
            throw new NucleusException("Cannot create MongoBooleanExpression with operator of " + op + " with this constructor");
        }
    }

    public MongoBooleanExpression(MongoBooleanExpression expr1, MongoBooleanExpression expr2, MongoOperator op)
    {
        if (op == MongoOperator.OP_AND)
        {
            BasicDBObject[] andOptions = new BasicDBObject[2];
            andOptions[0] = expr1.getDBObject();
            andOptions[1] = expr2.getDBObject();
            dbObject = new BasicDBObject(op.getValue(), andOptions);
        }
        else if (op == MongoOperator.OP_OR)
        {
            BasicDBObject[] orOptions = new BasicDBObject[2];
            orOptions[0] = expr1.getDBObject();
            orOptions[1] = expr2.getDBObject();
            dbObject = new BasicDBObject(op.getValue(), orOptions);
        }
        else
        {
            throw new NucleusException("Cannot create MongoBooleanExpression with operator of " + op + " with this constructor");
        }
    }

    private Object asEnumValue(MongoFieldExpression fieldExpr, Enum<?> value)
    {
        // Use the right type of Enum value for the property being compared against
        AbstractMemberMetaData mmd = fieldExpr.getMemberMetaData();
        ColumnMetaData colmd = null;
        if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
        {
            colmd = mmd.getColumnMetaData()[0];
        }
        if (MetaDataUtils.persistColumnAsNumeric(colmd))
        {
            return value.ordinal();
        }

        return value.toString();
    }


    public BasicDBObject getDBObject()
    {
        return dbObject;
    }

    public String toString()
    {
        return dbObject.toString();
    }
}