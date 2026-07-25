// Serverless Framework parsearia ".json" como objeto -- pero
// AWS::StepFunctions::StateMachine.DefinitionString necesita un STRING (para
// poder envolverlo en Fn::Sub y que CloudFormation reemplace los ARNs de las
// Lambdas ahi dentro). Este shim fuerza una lectura de texto plano del ASL,
// sin agregar el plugin serverless-step-functions solo para esto. Ver
// infra/stepfunctions.yml y docs/decisiones.md.
const fs = require("fs");
const path = require("path");

module.exports = fs.readFileSync(
  path.join(__dirname, "backfill.asl.json"),
  "utf8"
);
