import { OperatorFunction } from 'rxjs';

import { DataQuery, MetricFindValue } from './datasource';
import { DataFrame } from './dataFrame';

export interface VariableSupport<TQuery extends DataQuery = DataQuery> {
  toVariables: () => OperatorFunction<DataFrame[], MetricFindValue[]>;
}