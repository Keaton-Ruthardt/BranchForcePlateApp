#!/usr/bin/env python3
"""
PDF Report Generator for Athlete Performance Data
This script generates a PDF report using ReportLab based on command line arguments.
"""

import argparse
import sys
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, Flowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
import io
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np

class ColorRect(Flowable):
    def __init__(self, width, height, color):
        Flowable.__init__(self)
        self.width = width
        self.height = height
        self.color = color
    def draw(self):
        self.canv.setFillColor(self.color)
        self.canv.rect(0, 0, self.width, self.height, fill=1, stroke=0)

def normalize_values(values, target_max=100):
    """Normalize values to a 0-100 scale for better visualization"""
    if not values or all(v == 0 for v in values):
        return [0] * len(values)
    
    max_val = max(values)
    if max_val == 0:
        return [0] * len(values)
    
    return [v / max_val * target_max for v in values]

def create_report(athlete_name, test_date, composite_score, concentric_impulse, 
                 eccentric_rfd, peak_force, takeoff_power, rsi_modified, eccentric_impulse,
                 avg_composite_score, avg_concentric_impulse, avg_eccentric_rfd, avg_peak_force, avg_takeoff_power, avg_rsi_modified, avg_eccentric_impulse,
                 max_composite_score, max_concentric_impulse, max_eccentric_rfd, max_peak_force, max_takeoff_power, max_rsi_modified, max_eccentric_impulse,
                 percentile_composite_score, percentile_concentric_impulse, percentile_eccentric_rfd, percentile_peak_force, percentile_takeoff_power, percentile_rsi_modified, percentile_eccentric_impulse):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, 
                          leftMargin=0.6*inch, 
                          rightMargin=0.6*inch, 
                          topMargin=0.5*inch, 
                          bottomMargin=0.4*inch)
    story = []
    styles = getSampleStyleSheet()
    
    # Modern light corporate theme styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=14,
        spaceAfter=2,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#1976d2'),
        backColor=colors.white,
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=12,
        spaceAfter=3,
        spaceBefore=3,
        textColor=colors.HexColor('#1976d2'),
        backColor=colors.white,
    )
    label_style = ParagraphStyle(
        'Label',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        textColor=colors.HexColor('#424242'),
        backColor=colors.white,
    )
    value_style = ParagraphStyle(
        'Value',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=14,
        textColor=colors.HexColor('#1976d2'),
        backColor=colors.white,
    )
    
    def safe_float(val):
        try:
            return float(val)
        except Exception:
            print(f"Warning: Could not convert value to float: {val}", file=sys.stderr)
            return 0.0
    
    # Convert all values to float and print for debugging
    composite_score_f = safe_float(composite_score)
    concentric_impulse_f = safe_float(concentric_impulse)
    eccentric_rfd_f = safe_float(eccentric_rfd)
    peak_force_f = safe_float(peak_force)
    takeoff_power_f = safe_float(takeoff_power)
    rsi_modified_f = safe_float(rsi_modified)
    eccentric_impulse_f = safe_float(eccentric_impulse)
    avg_composite_score_f = safe_float(avg_composite_score)
    avg_concentric_impulse_f = safe_float(avg_concentric_impulse)
    avg_eccentric_rfd_f = safe_float(avg_eccentric_rfd)
    avg_peak_force_f = safe_float(avg_peak_force)
    avg_takeoff_power_f = safe_float(avg_takeoff_power)
    avg_rsi_modified_f = safe_float(avg_rsi_modified)
    avg_eccentric_impulse_f = safe_float(avg_eccentric_impulse)
    
    # Convert percentiles to float
    percentile_composite_score_f = safe_float(percentile_composite_score)
    percentile_concentric_impulse_f = safe_float(percentile_concentric_impulse)
    percentile_eccentric_rfd_f = safe_float(percentile_eccentric_rfd)
    percentile_peak_force_f = safe_float(percentile_peak_force)
    percentile_takeoff_power_f = safe_float(percentile_takeoff_power)
    percentile_rsi_modified_f = safe_float(percentile_rsi_modified)
    percentile_eccentric_impulse_f = safe_float(percentile_eccentric_impulse)
    
    print('PDF values:', composite_score_f, concentric_impulse_f, eccentric_rfd_f, peak_force_f, takeoff_power_f, rsi_modified_f, eccentric_impulse_f, avg_composite_score_f, avg_concentric_impulse_f, avg_eccentric_rfd_f, avg_peak_force_f, avg_takeoff_power_f, avg_rsi_modified_f, avg_eccentric_impulse_f, file=sys.stderr)
    print('Percentiles:', percentile_composite_score_f, percentile_concentric_impulse_f, percentile_eccentric_rfd_f, percentile_peak_force_f, percentile_takeoff_power_f, percentile_rsi_modified_f, percentile_eccentric_impulse_f, file=sys.stderr)
    
    # Ultra-compact header with athlete info
    story.append(Paragraph("Performance Report", title_style))
    story.append(Paragraph(f"<b>{athlete_name}</b> • {test_date}", 
                          ParagraphStyle('AthleteInfo', parent=styles['Normal'], 
                                       fontSize=11, textColor=colors.HexColor('#424242'), 
                                       alignment=TA_CENTER, spaceAfter=6)))
    story.append(Spacer(1, 4))
    
    # Create circular progress indicator with crisp rendering
    def create_circular_score(score, max_score=100):
        # Reset matplotlib to defaults and set only essential params
        plt.rcdefaults()
        plt.rcParams['figure.dpi'] = 300
        plt.rcParams['savefig.dpi'] = 300
        
        fig, ax = plt.subplots(figsize=(2.5, 2.5), facecolor='white', dpi=300)
        ax.set_xlim(-1.2, 1.2)
        ax.set_ylim(-1.2, 1.2)
        ax.set_aspect('equal')
        ax.axis('off')
        
        # Calculate progress percentage
        progress = min(score / max_score, 1.0)
        
        # Background circle (light gray) with crisp edges
        bg_circle = patches.Circle((0, 0), 1, linewidth=10, edgecolor='#e0e0e0', 
                                 facecolor='none', linestyle='-', antialiased=True)
        ax.add_patch(bg_circle)
        
        # Progress arc (corporate blue) with smooth rendering
        if progress > 0:
            theta1 = 90  # Start at top
            theta2 = 90 - (progress * 360)  # Go clockwise
            progress_arc = patches.Wedge((0, 0), 1, theta2, theta1, 
                                       width=0.10, facecolor='#1976d2', 
                                       edgecolor='#1976d2', linewidth=0,
                                       antialiased=True)
            ax.add_patch(progress_arc)
        
        # Score text in center (larger and more prominent) with crisp text
        ax.text(0, 0, f'{score:.1f}', fontsize=40, fontweight='bold', 
                ha='center', va='center', color='#1976d2', 
                fontfamily='sans-serif')
        
        plt.tight_layout()
        return fig
    
    # Generate and embed the circular score
    score_fig = create_circular_score(composite_score_f)
    score_buffer = io.BytesIO()
    score_fig.savefig(score_buffer, format='PNG', bbox_inches='tight', 
                     transparent=False, facecolor='white', dpi=200, pad_inches=0.1)
    plt.close(score_fig)
    score_buffer.seek(0)
    
    # Create side-by-side layout: Composite Score (left) and Metrics (right)
    main_content_data = [[
        Image(score_buffer, width=1.6*inch, height=1.6*inch),
        Table([
            ["Metric", "Value", "Percentile"],
            ["Composite Score", f"{composite_score_f:.1f}", f"{percentile_composite_score_f:.0f}%"],
            ["Concentric Impulse", f"{concentric_impulse_f:.0f}", f"{percentile_concentric_impulse_f:.0f}%"],
            ["Eccentric RFD", f"{eccentric_rfd_f:.0f}", f"{percentile_eccentric_rfd_f:.0f}%"],
            ["Peak Force", f"{peak_force_f:.0f}", f"{percentile_peak_force_f:.0f}%"],
            ["Takeoff Power", f"{takeoff_power_f:.1f}", f"{percentile_takeoff_power_f:.0f}%"],
            ["RSI Modified", f"{rsi_modified_f:.2f}", f"{percentile_rsi_modified_f:.0f}%"],
            ["Eccentric Impulse", f"{eccentric_impulse_f:.0f}", f"{percentile_eccentric_impulse_f:.0f}%"],
        ], colWidths=[1.8*inch, 0.6*inch, 0.6*inch], style=TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1976d2')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('RIGHTPADDING', (0, 0), (-1, -1), 2),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#424242')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e0e0e0')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')])
        ]))
    ]]
    
    main_content_table = Table(main_content_data, colWidths=[2*inch, 4*inch])
    main_content_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    
    story.append(main_content_table)
    story.append(Spacer(1, 6))
    
    # Professional Spider Chart (no header needed)
    
    # Parse max values from parameters
    max_composite_score_f = safe_float(max_composite_score)
    max_concentric_impulse_f = safe_float(max_concentric_impulse)
    max_eccentric_rfd_f = safe_float(max_eccentric_rfd)
    max_peak_force_f = safe_float(max_peak_force)
    max_takeoff_power_f = safe_float(max_takeoff_power)
    max_rsi_modified_f = safe_float(max_rsi_modified)
    max_eccentric_impulse_f = safe_float(max_eccentric_impulse)
    
    # Rearrange metrics for better label visibility (spread around circle) - including Composite Score
    metrics_config = [
        ("Composite Score", composite_score_f, avg_composite_score_f, 0, max_composite_score_f),
        ("Peak Force", peak_force_f, avg_peak_force_f, 0, max_peak_force_f),
        ("Takeoff Power", takeoff_power_f, avg_takeoff_power_f, 0, max_takeoff_power_f), 
        ("RSI Modified", rsi_modified_f, avg_rsi_modified_f, 0, max_rsi_modified_f),
        ("Eccentric Impulse", eccentric_impulse_f, avg_eccentric_impulse_f, 0, max_eccentric_impulse_f),
        ("Eccentric RFD", eccentric_rfd_f, avg_eccentric_rfd_f, 0, max_eccentric_rfd_f),
        ("Concentric Impulse", concentric_impulse_f, avg_concentric_impulse_f, 0, max_concentric_impulse_f),
    ]
    
    # Normalize values to 0-100 scale for each metric based on their expected ranges
    labels = []
    test_values_normalized = []
    avg_values_normalized = []
    
    for label, test_val, avg_val, min_range, max_range in metrics_config:
        labels.append(label)
        
        # Normalize test value
        if max_range > min_range:
            test_normalized = max(0, min(100, (test_val - min_range) / (max_range - min_range) * 100))
        else:
            test_normalized = 0
        test_values_normalized.append(test_normalized)
        
        # Normalize average value
        if max_range > min_range:
            avg_normalized = max(0, min(100, (avg_val - min_range) / (max_range - min_range) * 100))
        else:
            avg_normalized = 0
        avg_values_normalized.append(avg_normalized)
    
    # Create professional executive-level radar chart
    angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
    test_values_normalized += test_values_normalized[:1]
    avg_values_normalized += avg_values_normalized[:1]
    angles += angles[:1]
    
    # Create figure optimized for crisp executive presentation
    # Reset matplotlib to defaults and set only essential params
    plt.rcdefaults()
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['path.simplify'] = False
    
    fig = plt.figure(figsize=(8, 6), facecolor='white', dpi=300)
    ax = plt.subplot(111, polar=True)
    ax.set_facecolor('white')
    
    # Sharp, clean grid styling with optimal line weights
    ax.set_ylim(0, 100)
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels([])  # Remove radial tick labels for clean look
    ax.grid(True, color='#e8e8e8', alpha=0.7, linewidth=0.8, linestyle='-', antialiased=True)
    
    # Subtle radial guides with crisp rendering
    ax.set_rgrids([20, 40, 60, 80, 100], labels=[], angle=0, alpha=0.3)
    
    # Professional axis labels with optimal spacing and crisp text
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, color='#2c3e50', fontsize=10, fontweight='600', 
                       fontfamily='sans-serif', ha='center')
    
    # Modern, clean data visualization with optimized line weights
    # Test Result area (primary - corporate blue) with crisp edges
    ax.fill(angles, test_values_normalized, color='#1976d2', alpha=0.25, label='Test Result', 
            linewidth=0, antialiased=True, rasterized=False)
    ax.plot(angles, test_values_normalized, color='#1976d2', linewidth=2.5, alpha=1.0, 
            solid_capstyle='round', solid_joinstyle='round', antialiased=True)
    
    # Average line (benchmark - professional contrast) with sharp rendering
    ax.plot(angles, avg_values_normalized, color='#ff6f00', linewidth=2.0, 
            label='Database Average', linestyle='--', alpha=1.0,
            solid_capstyle='round', antialiased=True)
    
    # Minimal accent points for clarity with crisp edges
    for i, (angle, test_val, avg_val) in enumerate(zip(angles[:-1], test_values_normalized[:-1], avg_values_normalized[:-1])):
        ax.plot(angle, test_val, 'o', color='#1976d2', markersize=5, markeredgewidth=1.5, 
                markeredgecolor='white', zorder=10)
        ax.plot(angle, avg_val, 's', color='#ff6f00', markersize=4, markeredgewidth=1.5, 
                markeredgecolor='white', zorder=10)
    
    # Clean, minimal legend
    legend = ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), 
                      ncol=2, fontsize=11, frameon=False,
                      columnspacing=2, handlelength=2, handletextpad=0.8)
    
    # Style the legend text
    for text in legend.get_texts():
        text.set_fontweight('600')
        text.set_color('#2c3e50')
    
    # Remove the polar plot border for cleaner look
    ax.spines['polar'].set_visible(False)
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.08)  # Optimized spacing
    
    # Save the chart with maximum sharpness and quality
    img_buffer = io.BytesIO()
    fig.savefig(img_buffer, format='PNG', bbox_inches='tight', transparent=False, 
                facecolor='white', edgecolor='none', dpi=300,
                pad_inches=0.1, pil_kwargs={'optimize': True, 'quality': 95})
    plt.close(fig)
    img_buffer.seek(0)
    
    # Add professional spider chart
    story.append(Image(img_buffer, width=340, height=240))
    story.append(Spacer(1, 2))
    
    # Compact footer
    story.append(Spacer(1, 2))
    story.append(Paragraph("<font size=8 color='#757575'>Generated by Push Performance Analytics</font>", label_style))
    
    doc.build(story)
    pdf_content = buffer.getvalue()
    buffer.close()
    return pdf_content

def main():
    parser = argparse.ArgumentParser(description='Generate PDF report for athlete performance data')
    parser.add_argument('--athlete-name', required=True, help='Athlete name')
    parser.add_argument('--test-date', required=True, help='Test date')
    parser.add_argument('--composite-score', required=True, help='Composite score')
    parser.add_argument('--concentric-impulse', required=True, help='Concentric impulse')
    parser.add_argument('--eccentric-rfd', required=True, help='Eccentric braking RFD')
    parser.add_argument('--peak-force', required=True, help='Peak concentric force')
    parser.add_argument('--takeoff-power', required=True, help='Body mass relative takeoff power')
    parser.add_argument('--rsi-modified', required=True, help='RSI modified')
    parser.add_argument('--eccentric-impulse', required=True, help='Eccentric braking impulse')
    parser.add_argument('--avg-composite-score', required=True, help='Average composite score')
    parser.add_argument('--avg-concentric-impulse', required=True, help='Average concentric impulse')
    parser.add_argument('--avg-eccentric-rfd', required=True, help='Average eccentric braking RFD')
    parser.add_argument('--avg-peak-force', required=True, help='Average peak concentric force')
    parser.add_argument('--avg-takeoff-power', required=True, help='Average takeoff power')
    parser.add_argument('--avg-rsi-modified', required=True, help='Average RSI modified')
    parser.add_argument('--avg-eccentric-impulse', required=True, help='Average eccentric braking impulse')
    parser.add_argument('--max-composite-score', required=True, help='Max composite score')
    parser.add_argument('--max-concentric-impulse', required=True, help='Max concentric impulse')
    parser.add_argument('--max-eccentric-rfd', required=True, help='Max eccentric braking RFD')
    parser.add_argument('--max-peak-force', required=True, help='Max peak concentric force')
    parser.add_argument('--max-takeoff-power', required=True, help='Max takeoff power')
    parser.add_argument('--max-rsi-modified', required=True, help='Max RSI modified')
    parser.add_argument('--max-eccentric-impulse', required=True, help='Max eccentric braking impulse')
    parser.add_argument('--percentile-composite-score', required=True, help='Percentile composite score')
    parser.add_argument('--percentile-concentric-impulse', required=True, help='Percentile concentric impulse')
    parser.add_argument('--percentile-eccentric-rfd', required=True, help='Percentile eccentric braking RFD')
    parser.add_argument('--percentile-peak-force', required=True, help='Percentile peak concentric force')
    parser.add_argument('--percentile-takeoff-power', required=True, help='Percentile takeoff power')
    parser.add_argument('--percentile-rsi-modified', required=True, help='Percentile RSI modified')
    parser.add_argument('--percentile-eccentric-impulse', required=True, help='Percentile eccentric braking impulse')
    args = parser.parse_args()
    
    try:
        # Generate the PDF
        pdf_content = create_report(
            args.athlete_name,
            args.test_date,
            args.composite_score,
            args.concentric_impulse,
            args.eccentric_rfd,
            args.peak_force,
            args.takeoff_power,
            args.rsi_modified,
            args.eccentric_impulse,
            args.avg_composite_score,
            args.avg_concentric_impulse,
            args.avg_eccentric_rfd,
            args.avg_peak_force,
            args.avg_takeoff_power,
            args.avg_rsi_modified,
            args.avg_eccentric_impulse,
            args.max_composite_score,
            args.max_concentric_impulse,
            args.max_eccentric_rfd,
            args.max_peak_force,
            args.max_takeoff_power,
            args.max_rsi_modified,
            args.max_eccentric_impulse,
            args.percentile_composite_score,
            args.percentile_concentric_impulse,
            args.percentile_eccentric_rfd,
            args.percentile_peak_force,
            args.percentile_takeoff_power,
            args.percentile_rsi_modified,
            args.percentile_eccentric_impulse
        )
        
        # Write PDF to stdout (Node.js will capture this)
        sys.stdout.buffer.write(pdf_content)
        sys.stdout.buffer.flush()
        
    except Exception as e:
        print(f"Error generating PDF: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 