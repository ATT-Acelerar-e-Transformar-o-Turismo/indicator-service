from fastapi import HTTPException, status


class IndicatorServiceException(Exception):
    """Base exception for indicator-service."""
    pass


class DomainNotFoundError(IndicatorServiceException):
    """Raised when a domain is not found."""
    pass


class SubdomainNotFoundError(IndicatorServiceException):
    """Raised when a subdomain is not found."""
    pass


class IndicatorNotFoundError(IndicatorServiceException):
    """Raised when an indicator is not found."""
    pass


class ResourceNotFoundError(IndicatorServiceException):
    """Raised when a resource is not found."""
    pass


class CacheException(IndicatorServiceException):
    """Raised when Redis cache operation fails."""
    pass


class DatabaseConnectionError(IndicatorServiceException):
    """Raised when database connection fails."""
    pass


def domain_not_found(domain_id: str) -> HTTPException:
    """Create HTTPException for domain not found."""
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Domain with id {domain_id} not found"
    )


def subdomain_not_found(subdomain_name: str) -> HTTPException:
    """Create HTTPException for subdomain not found."""
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Subdomain {subdomain_name} not found"
    )


def indicator_not_found(indicator_id: str) -> HTTPException:
    """Create HTTPException for indicator not found."""
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Indicator with id {indicator_id} not found"
    )


def resource_not_found(resource_id: str) -> HTTPException:
    """Create HTTPException for resource not found."""
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Resource with id {resource_id} not found"
    )
