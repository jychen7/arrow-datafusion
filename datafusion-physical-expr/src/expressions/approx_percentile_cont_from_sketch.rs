// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::Literal;
use crate::expressions::approx_percentile_cont::ApproxPercentileAccumulator;
use crate::expressions::ApproxPercentileCont;
use crate::{tdigest::TDigest, AggregateExpr, PhysicalExpr};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field},
};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use std::{any::Any, sync::Arc};

#[derive(Debug, Clone)]
pub enum SketchType {
    TDigest,
}

impl SketchType {
    fn from_str(value: String) -> Result<SketchType> {
        if value == "tdigest" {
            return Ok(SketchType::TDigest);
        }
        Err(DataFusionError::Internal(
            "desired sketch_type should be one of [tdigest]".to_string(),
        ))
    }
}
pub fn is_approx_percentile_cont_from_sketch_supported_arg_type(
    arg_type: &DataType,
) -> bool {
    matches!(arg_type, DataType::Utf8 | DataType::LargeUtf8)
}

#[derive(Debug)]
pub struct ApproxPercentileContFromSketch {
    approx_percentile_cont: ApproxPercentileCont,
    sketch_type: SketchType,
}

impl ApproxPercentileContFromSketch {
    pub fn new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        _input_data_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, DesiredPercentileLiteral, SketchTypeLiterral]
        debug_assert_eq!(expr.len(), 3);

        let heads = expr[0..=1].to_vec();
        let tail = &expr[2];
        let approx_percentile_cont =
            ApproxPercentileCont::new(heads, name, DataType::Float64)?;
        // Extract the sketch_type literal
        let lit = tail
            .as_any()
            .downcast_ref::<Literal>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "desired sketch_type argument must be Utf8 literal".to_string(),
                )
            })?
            .value();
        let sketch_type_value = match lit {
            ScalarValue::Utf8(Some(str)) => &*str,
            ScalarValue::LargeUtf8(Some(str)) => &*str,
            got => return Err(DataFusionError::NotImplemented(format!(
                "sketch_type value for 'APPROX_PERCENTILE_CONT_FROM_SKETCH' must be Utf8 literal (got data type {})",
                got
            )))
        };

        let sketch_type = SketchType::from_str(sketch_type_value.to_string())?;
        Ok(Self {
            approx_percentile_cont,
            sketch_type,
        })
    }
}

impl AggregateExpr for ApproxPercentileContFromSketch {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.approx_percentile_cont.name(),
            DataType::Float64,
            false,
        ))
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// See [`TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(&self) -> Result<Vec<Field>> {
        self.approx_percentile_cont.state_fields()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.approx_percentile_cont.expressions()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator: Box<dyn Accumulator> =
            Box::new(ApproxPercentileFromSketchAccumulator::new(
                self.sketch_type.clone(),
                self.approx_percentile_cont.percentile(),
                DataType::Float64,
            ));
        Ok(accumulator)
    }

    fn name(&self) -> &str {
        &self.approx_percentile_cont.name()
    }
}

#[derive(Debug)]
pub struct ApproxPercentileFromSketchAccumulator {
    approx_percentile_cont_accumulator: Option<ApproxPercentileAccumulator>,
    sketch_type: SketchType,
    percentile: f64,
    return_type: DataType,
}

impl ApproxPercentileFromSketchAccumulator {
    pub fn new(sketch_type: SketchType, percentile: f64, return_type: DataType) -> Self {
        Self {
            approx_percentile_cont_accumulator: None,
            sketch_type: sketch_type,
            percentile: percentile,
            return_type: return_type,
        }
    }
}

impl Accumulator for ApproxPercentileFromSketchAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        self.approx_percentile_cont_accumulator
            .as_ref()
            .unwrap()
            .state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        debug_assert_eq!(
            values.len(),
            1,
            "invalid number of values in batch percentile update"
        );
        let values = &values[0];

        let mut digests: Vec<TDigest> = vec![];
        match values.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let array = values.as_any().downcast_ref::<StringArray>().unwrap();
                for element in array {
                    match element {
                        Some(serialized_sketch) => {
                            let digest: TDigest =
                                TDigest::from_utf8(serialized_sketch.to_string());
                            digests.push(digest);
                        }
                        _ => (),
                    }
                }
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "APPROX_PERCENTILE_CONT is not expected to receive the type {:?}",
                    e
                )));
            }
        };

        if !self.approx_percentile_cont_accumulator.is_none() {
            digests.push(
                self.approx_percentile_cont_accumulator
                    .as_ref()
                    .unwrap()
                    .digest(),
            )
        }
        let merged_digest = TDigest::merge_digests(&digests);
        self.approx_percentile_cont_accumulator =
            Some(ApproxPercentileAccumulator::new_with_digest(
                merged_digest,
                self.percentile,
                self.return_type.clone(),
            ));
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        self.approx_percentile_cont_accumulator
            .as_ref()
            .unwrap()
            .evaluate()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        if self.approx_percentile_cont_accumulator.is_none() {
            self.approx_percentile_cont_accumulator =
                Some(ApproxPercentileAccumulator::new(
                    TDigest::DEFAULT_COMPRESSION,
                    self.percentile,
                    self.return_type.clone(),
                ));
        }
        let merged_digest = self
            .approx_percentile_cont_accumulator
            .as_ref()
            .unwrap()
            .merge_non_empty_batch(states)?;
        self.approx_percentile_cont_accumulator =
            Some(ApproxPercentileAccumulator::new_with_digest(
                merged_digest,
                self.percentile,
                self.return_type.clone(),
            ));

        Ok(())
    }
}
