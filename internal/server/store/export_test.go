package store

// This file exports internal identifiers to the external store_test package.

// NewMetricIDForTesting returns the metricID of the metric named name, owned
// by recommenderName (empty for policy-wide metrics). It lets external tests
// index PolicyState.Series and PolicyState.GlobalHistograms.
func NewMetricIDForTesting(name, recommenderName string) metricID {
	return metricID{name: name, recommenderName: recommenderName}
}
