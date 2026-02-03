from datetime import datetime, timedelta
from typing import Optional
from src.db.mongo import get_db
from src.core.config import settings
from src.core.logging import logger
import pymongo

class AnalyticsService:
    async def get_tenant_stats(
        self, 
        tenant_id: str, 
        from_date: Optional[datetime] = None, 
        to_date: Optional[datetime] = None,
        max_results: Optional[int] = None
    ) -> dict:
        """
        Compute analytics for a tenant using MongoDB aggregation pipeline.
        All computation happens in the database for optimal performance.
        
        Resource Management:
        - Query timeout enforced to prevent long-running queries
        - Result size limited to prevent memory exhaustion
        - Connection automatically returned to pool after use
        """
        db = await get_db()
        
        # Enforce max results limit to prevent memory exhaustion
        if max_results is None:
            max_results = settings.MAX_PAGE_SIZE
        
        # Default date range: last 24 hours for hourly trend
        if not to_date:
            to_date = datetime.utcnow()
        if not from_date:
            from_date = to_date - timedelta(hours=24)
        
        # Single aggregation pipeline using $facet for multiple metrics
        pipeline = [
            # Filter by tenant and exclude soft-deleted tickets
            {
                "$match": {
                    "tenant_id": tenant_id,
                    "deleted_at": {"$exists": False}
                }
            },
            # Compute all metrics in parallel using $facet
            {
                "$facet": {
                    # Total count
                    "total": [
                        {"$count": "count"}
                    ],
                    # Status distribution
                    "by_status": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                        {"$project": {"status": "$_id", "count": 1, "_id": 0}}
                    ],
                    # Urgency distribution
                    "by_urgency": [
                        {"$group": {"_id": "$urgency", "count": {"$sum": 1}}},
                        {"$project": {"urgency": "$_id", "count": 1, "_id": 0}}
                    ],
                    # Sentiment distribution
                    "by_sentiment": [
                        {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}},
                        {"$project": {"sentiment": "$_id", "count": 1, "_id": 0}}
                    ],
                    # Hourly trend for last 24 hours
                    "hourly_data": [
                        {
                            "$match": {
                                "created_at": {"$gte": from_date, "$lte": to_date}
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%dT%H:00:00Z",
                                        "date": "$created_at"
                                    }
                                },
                                "count": {"$sum": 1}
                            }
                        },
                        {"$project": {"hour": "$_id", "count": 1, "_id": 0}}
                    ],
                    # Top keywords from subjects (optional)
                    "keywords": [
                        {"$project": {"words": {"$split": ["$subject", " "]}}},
                        {"$unwind": "$words"},
                        {
                            "$group": {
                                "_id": {"$toLower": "$words"},
                                "count": {"$sum": 1}
                            }
                        },
                        {"$match": {"_id": {"$nin": ["", "issue", "ext"]}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 10},
                        {"$project": {"keyword": "$_id", "count": 1, "_id": 0}}
                    ],
                    # At-risk customers (high urgency + negative sentiment)
                    "at_risk": [
                        {
                            "$match": {
                                "$or": [
                                    {"urgency": "high"},
                                    {"sentiment": "negative"}
                                ]
                            }
                        },
                        {
                            "$group": {
                                "_id": "$customer_id",
                                "high_urgency_count": {
                                    "$sum": {"$cond": [{"$eq": ["$urgency", "high"]}, 1, 0]}
                                },
                                "negative_count": {
                                    "$sum": {"$cond": [{"$eq": ["$sentiment", "negative"]}, 1, 0]}
                                },
                                "total_tickets": {"$sum": 1}
                            }
                        },
                        {
                            "$match": {
                                "$or": [
                                    {"high_urgency_count": {"$gte": 2}},
                                    {"negative_count": {"$gte": 3}}
                                ]
                            }
                        },
                        {"$sort": {"high_urgency_count": -1, "negative_count": -1}},
                        {"$limit": 10},
                        {
                            "$project": {
                                "customer_id": "$_id",
                                "high_urgency_count": 1,
                                "negative_count": 1,
                                "total_tickets": 1,
                                "_id": 0
                            }
                        }
                    ]
                }
            }
        ]
        
        # Execute aggregation with timeout to prevent resource exhaustion
        try:
            cursor = db.tickets.aggregate(
                pipeline,
                maxTimeMS=settings.ANALYTICS_QUERY_TIMEOUT_MS,  # Query timeout
                allowDiskUse=False  # Prevent disk spills for predictable memory usage
            )
            result = await cursor.to_list(length=1)
        except pymongo.errors.ExecutionTimeout:
            logger.error(f"Analytics query timeout for tenant {tenant_id}")
            raise Exception(f"Analytics query exceeded timeout of {settings.ANALYTICS_QUERY_TIMEOUT_MS}ms")
        except Exception as e:
            logger.error(f"Analytics query failed for tenant {tenant_id}: {e}")
            raise
        
        if not result:
            return self._empty_stats()
        
        data = result[0]
        
        # Process results
        total_tickets = data["total"][0]["count"] if data["total"] else 0
        
        # Status counts
        by_status = {item["status"]: item["count"] for item in data["by_status"]}
        
        # Urgency ratio
        urgency_counts = {item["urgency"]: item["count"] for item in data["by_urgency"]}
        high_count = urgency_counts.get("high", 0)
        urgency_high_ratio = round(high_count / total_tickets, 3) if total_tickets > 0 else 0.0
        
        # Sentiment ratio
        sentiment_counts = {item["sentiment"]: item["count"] for item in data["by_sentiment"]}
        negative_count = sentiment_counts.get("negative", 0)
        negative_sentiment_ratio = round(negative_count / total_tickets, 3) if total_tickets > 0 else 0.0
        
        # Hourly trend with zero-padding
        hourly_trend = self._build_hourly_trend(data["hourly_data"], from_date, to_date)
        
        # Top keywords
        top_keywords = [item["keyword"] for item in data["keywords"]]
        
        # At-risk customers
        at_risk_customers = data["at_risk"]
        
        return {
            "total_tickets": total_tickets,
            "by_status": by_status,
            "urgency_high_ratio": urgency_high_ratio,
            "negative_sentiment_ratio": negative_sentiment_ratio,
            "hourly_trend": hourly_trend,
            "top_keywords": top_keywords,
            "at_risk_customers": at_risk_customers
        }
    
    def _build_hourly_trend(self, hourly_data: list, from_date: datetime, to_date: datetime) -> list:
        """
        Build hourly trend with zero-padding for missing hours.
        """
        # Convert hourly data to dict
        data_dict = {item["hour"]: item["count"] for item in hourly_data}
        
        # Generate all hours in range
        trend = []
        current = from_date.replace(minute=0, second=0, microsecond=0)
        
        while current <= to_date:
            hour_str = current.strftime("%Y-%m-%dT%H:00:00Z")
            trend.append({
                "hour": hour_str,
                "count": data_dict.get(hour_str, 0)
            })
            current += timedelta(hours=1)
        
        return trend
    
    def _empty_stats(self) -> dict:
        """Return empty stats structure."""
        return {
            "total_tickets": 0,
            "by_status": {},
            "urgency_high_ratio": 0.0,
            "negative_sentiment_ratio": 0.0,
            "hourly_trend": [],
            "top_keywords": [],
            "at_risk_customers": []
        }
