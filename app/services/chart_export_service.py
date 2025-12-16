from typing import List, Dict, Any, Optional
import plotly.graph_objects as go
import os
from datetime import datetime
from schemas.chart_export import ChartExportRequest, ChartType, ThemeMode
from services.data_propagator import get_data_points
from fastapi import BackgroundTasks
import base64

def load_base64_image(path: str) -> str:
    with open(path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")
    return "data:image/png;base64," + encoded

class ChartExportService:
    async def generate_chart_image(
        self, 
        request: ChartExportRequest, 
        indicator_id: str,
        background_tasks: BackgroundTasks
    ) -> bytes:
        # 1. Fetch Data
        data_points = await get_data_points(
            indicator_id=indicator_id,
            granularity=request.granularity,
            start_date=request.start_date,
            end_date=request.end_date,
            background_tasks=background_tasks
        )
        
        if not data_points:
            # Return empty chart or raise error? Let's return an empty chart with message
            return self._create_empty_chart_image(request)

        # 2. Prepare Data for Plotly
        x_values = [dp.x for dp in data_points]
        y_values = [dp.y for dp in data_points]

        # 3. Create Figure
        fig = go.Figure()

        # Add Trace based on chart type
        if request.chart_type == ChartType.line:
            fig.add_trace(go.Scatter(x=x_values, y=y_values, mode='lines', line=dict(color=request.colors[0] if request.colors else None)))
        elif request.chart_type == ChartType.area:
            fig.add_trace(go.Scatter(x=x_values, y=y_values, mode='lines', fill='tozeroy', line=dict(color=request.colors[0] if request.colors else None)))
        elif request.chart_type == ChartType.bar:
            fig.add_trace(go.Bar(x=y_values, y=x_values, orientation='h', marker_color=request.colors[0] if request.colors else None)) # Swap x/y for horizontal bar
        elif request.chart_type == ChartType.column:
            fig.add_trace(go.Bar(x=x_values, y=y_values, marker_color=request.colors[0] if request.colors else None))
        elif request.chart_type == ChartType.scatter:
            fig.add_trace(go.Scatter(x=x_values, y=y_values, mode='markers', marker=dict(color=request.colors[0] if request.colors else None)))

        # 4. Apply Layout & Styling
        template = "plotly_white" if request.theme == ThemeMode.light else "plotly_dark"
        
        layout_args = {
            "template": template,
            "width": request.width,
            "height": request.height,
            "title": dict(text=request.title or "", x=0.5),
            "margin": dict(l=40, r=40, t=60, b=40),
            "xaxis": dict(title="Date" if request.xaxis_type == "datetime" else ""), # Simple default
            "yaxis": dict(title="Value"),
        }
        
        # Apply Annotations if present
        annotations_list = []
        if request.annotations:
            # X-axis annotations (Vertical lines)
            if "xaxis" in request.annotations and request.annotations["xaxis"]:
                for ann in request.annotations["xaxis"]:
                    fig.add_vline(x=ann["value"], line_dash="dash", line_color="gray")
                    annotations_list.append(dict(
                        x=ann["value"], y=1, yref="paper",
                        text=ann["label"] or "",
                        showarrow=False,
                        font=dict(size=10, color="gray")
                    ))
            
            # Y-axis annotations (Horizontal lines)
            if "yaxis" in request.annotations and request.annotations["yaxis"]:
                for ann in request.annotations["yaxis"]:
                    fig.add_hline(y=ann["value"], line_dash="dash", line_color="gray")
                    annotations_list.append(dict(
                        x=1, xref="paper", y=ann["value"],
                        text=ann["label"] or "",
                        showarrow=False,
                        font=dict(size=10, color="gray")
                    ))
        
        # add a watermark image to it the image
        fig.add_layout_image(
            dict(
                source=load_base64_image(os.path.join("assets", "verde.png")),
                xref="paper",
                yref="paper",
                x=0.99,
                y=-0.07,
                sizex=0.1,
                sizey=0.1,
                xanchor="center",
                yanchor="middle",
                opacity=1,
                layer="below"
            )
        )
            
        if annotations_list:
            layout_args["annotations"] = annotations_list

        fig.update_layout(**layout_args)

        # 5. Render to Image
        img_bytes = fig.to_image(format="png", scale=2)
        return img_bytes

    def _create_empty_chart_image(self, request: ChartExportRequest) -> bytes:
        fig = go.Figure()
        fig.update_layout(
            width=request.width,
            height=request.height,
            xaxis={"visible": False},
            yaxis={"visible": False},
            annotations=[
                {
                    "text": "No Data Available",
                    "xref": "paper",
                    "yref": "paper",
                    "showarrow": False,
                    "font": {"size": 20}
                }
            ]
        )
        return fig.to_image(format="png")

export_service = ChartExportService()
