package uk.co.devworx.cloudfront;

/**
 * Represents the x-edge-result-type in the Cloudfront log.,
 */
public enum XEdgeResultType
{
	Hit,
	RefreshHit,
	OriginShieldHit,
	Miss,
	LimitExceeded,
	CapacityExceeded,
	Error,
	Redirect;
}
