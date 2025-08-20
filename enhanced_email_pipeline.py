"""
Enhanced Data Jobs Pipeline with Email Dashboard
Sends beautiful HTML emails with embedded charts
Author: Eman Elgamal
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import glob
from dotenv import load_dotenv

load_dotenv()

def create_dashboard_charts(jobs_data):
    """Create beautiful charts and save as images"""
    
    print("📊 Creating dashboard charts...")
    
    df = pd.DataFrame(jobs_data)
    
    # Create charts directory
    os.makedirs('reports/charts', exist_ok=True)
    
    chart_files = []
    
    try:
        # Chart 1: Jobs by Category
        category_counts = df.groupby('category').size().reset_index(name='count')
        category_counts['category_display'] = category_counts['category'].str.replace('_', ' ').str.title()
        
        fig1 = px.bar(
            category_counts, 
            x='category_display', 
            y='count',
            title="📊 Jobs by Category",
            color='count',
            color_continuous_scale='viridis',
            text='count'
        )
        fig1.update_traces(texttemplate='%{text}', textposition='outside')
        fig1.update_layout(
            font=dict(size=14),
            title_font_size=20,
            showlegend=False,
            height=500
        )
        
        chart1_path = 'reports/charts/jobs_by_category.png'
        fig1.write_image(chart1_path, width=900, height=500)
        chart_files.append(('Jobs by Category', chart1_path))
        
        # Chart 2: Work Mode Distribution
        work_mode_counts = df['work_mode'].value_counts()
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
        
        fig2 = px.pie(
            values=work_mode_counts.values,
            names=work_mode_counts.index,
            title="🏠 Work Mode Distribution",
            color_discrete_sequence=colors
        )
        fig2.update_traces(textposition='inside', textinfo='percent+label', textfont_size=14)
        fig2.update_layout(
            font=dict(size=14),
            title_font_size=20,
            height=500
        )
        
        chart2_path = 'reports/charts/work_mode_distribution.png'
        fig2.write_image(chart2_path, width=900, height=500)
        chart_files.append(('Work Mode Distribution', chart2_path))
        
        # Chart 3: Experience Level Distribution
        exp_counts = df['experience_level'].value_counts()
        
        fig3 = px.bar(
            x=exp_counts.values,
            y=exp_counts.index,
            orientation='h',
            title="👥 Experience Level Distribution",
            color=exp_counts.values,
            color_continuous_scale='blues',
            text=exp_counts.values
        )
        fig3.update_traces(texttemplate='%{text}', textposition='outside')
        fig3.update_layout(
            font=dict(size=14),
            title_font_size=20,
            showlegend=False,
            height=400
        )
        
        chart3_path = 'reports/charts/experience_level.png'
        fig3.write_image(chart3_path, width=900, height=400)
        chart_files.append(('Experience Level', chart3_path))
        
        # Chart 4: Top Companies (if available)
        if df['company'].nunique() > 1 and len(df[df['company'] != 'N/A']) > 0:
            company_counts = df[df['company'] != 'N/A']['company'].value_counts().head(10)
            
            fig4 = px.bar(
                x=company_counts.values,
                y=company_counts.index,
                orientation='h',
                title="🏢 Top Hiring Companies",
                color=company_counts.values,
                color_continuous_scale='greens',
                text=company_counts.values
            )
            fig4.update_traces(texttemplate='%{text}', textposition='outside')
            fig4.update_layout(
                font=dict(size=12),
                title_font_size=20,
                showlegend=False,
                height=500
            )
            
            chart4_path = 'reports/charts/top_companies.png'
            fig4.write_image(chart4_path, width=900, height=500)
            chart_files.append(('Top Companies', chart4_path))
        
        print(f"✅ Created {len(chart_files)} dashboard charts")
        return chart_files
        
    except Exception as e:
        print(f"❌ Chart creation failed: {e}")
        print("💡 Make sure to install: pip install kaleido")
        return []

def send_enhanced_dashboard_email(jobs_data, analytics):
    """Send beautiful HTML email with dashboard charts"""
    
    print("\n📧 Creating Enhanced Dashboard Email...")
    
    try:
        # Email configuration
        sender_email = os.getenv('SENDER_EMAIL')
        sender_password = os.getenv('SENDER_PASSWORD')
        recipient_email = os.getenv('RECIPIENT_EMAIL')
        
        if not all([sender_email, sender_password, recipient_email]):
            print("❌ Email configuration incomplete")
            return False
        
        # Create charts
        chart_files = create_dashboard_charts(jobs_data)
        
        # Create email
        msg = MIMEMultipart('related')
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"📊 Data Jobs Intelligence Dashboard - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Create HTML email body
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    line-height: 1.6; 
                    color: #333; 
                    max-width: 800px;
                    margin: 0 auto;
                    background-color: #f8f9fa;
                }}
                .container {{
                    background-color: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                    margin: 20px;
                }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; 
                    padding: 30px; 
                    text-align: center; 
                    border-radius: 10px;
                    margin-bottom: 30px;
                }}
                .metric {{ 
                    background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    color: white;
                    padding: 20px; 
                    margin: 10px; 
                    border-radius: 10px; 
                    display: inline-block; 
                    min-width: 150px;
                    text-align: center;
                    font-weight: bold;
                }}
                .section {{ 
                    margin: 30px 0; 
                    padding: 20px;
                    background-color: #f8f9fa;
                    border-radius: 10px;
                }}
                .chart-container {{
                    text-align: center;
                    margin: 20px 0;
                    padding: 15px;
                    background-color: white;
                    border-radius: 10px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .chart-image {{
                    max-width: 100%;
                    height: auto;
                    border-radius: 8px;
                }}
                .insight {{
                    background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
                    padding: 15px;
                    border-radius: 10px;
                    margin: 15px 0;
                    border-left: 5px solid #007bff;
                }}
                .footer {{
                    text-align: center;
                    padding: 20px;
                    color: #666;
                    border-top: 1px solid #eee;
                    margin-top: 30px;
                }}
                h1, h2, h3 {{ color: #2c3e50; }}
                .emoji {{ font-size: 1.2em; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1><span class="emoji">📊</span> Data Jobs Intelligence Dashboard</h1>
                    <p style="font-size: 18px; margin: 0;">Daily Market Report</p>
                    <p style="opacity: 0.9;">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="section">
                    <h2><span class="emoji">📈</span> Executive Summary</h2>
                    <div style="text-align: center;">
                        <div class="metric">
                            <div style="font-size: 24px;">{analytics.get('total_jobs', 0)}</div>
                            <div>Total Jobs</div>
                        </div>
                        <div class="metric">
                            <div style="font-size: 24px;">{len(analytics.get('jobs_by_category', {}))}</div>
                            <div>Categories</div>
                        </div>
                        <div class="metric">
                            <div style="font-size: 24px;">{analytics.get('jobs_by_work_mode', {}).get('Remote', 0)}</div>
                            <div>Remote Jobs</div>
                        </div>
                        <div class="metric">
                            <div style="font-size: 24px;">{analytics.get('jobs_by_experience', {}).get('Senior Level', 0)}</div>
                            <div>Senior Positions</div>
                        </div>
                    </div>
                </div>
        """
        
        # Add charts to email
        for chart_name, chart_path in chart_files:
            if os.path.exists(chart_path):
                html_body += f"""
                <div class="chart-container">
                    <h3>{chart_name}</h3>
                    <img src="cid:{chart_name.lower().replace(' ', '_')}" class="chart-image" alt="{chart_name}">
                </div>
                """
        
        # Add insights section
        html_body += f"""
                <div class="section">
                    <h2><span class="emoji">🔍</span> Key Insights</h2>
        """
        
        # Generate insights
        if analytics.get('jobs_by_category'):
            top_category = max(analytics['jobs_by_category'], key=analytics['jobs_by_category'].get)
            html_body += f"""
                    <div class="insight">
                        <strong>Most In-Demand:</strong> {top_category.replace('_', ' ').title()} positions lead the market with {analytics['jobs_by_category'][top_category]} openings.
                    </div>
            """
        
        if analytics.get('jobs_by_work_mode'):
            remote_count = analytics['jobs_by_work_mode'].get('Remote', 0)
            total_jobs = analytics.get('total_jobs', 1)
            remote_pct = (remote_count / total_jobs) * 100
            html_body += f"""
                    <div class="insight">
                        <strong>Remote Work Trend:</strong> {remote_pct:.1f}% of positions offer remote work options ({remote_count} out of {total_jobs} jobs).
                    </div>
            """
        
        if analytics.get('jobs_by_experience'):
            senior_count = analytics['jobs_by_experience'].get('Senior Level', 0)
            html_body += f"""
                    <div class="insight">
                        <strong>Experience Demand:</strong> {senior_count} senior-level positions available, indicating strong demand for experienced professionals.
                    </div>
            """
        
        # Add detailed breakdown
        html_body += f"""
                </div>
                
                <div class="section">
                    <h2><span class="emoji">📊</span> Detailed Breakdown</h2>
                    
                    <h3>Jobs by Category:</h3>
                    <ul style="list-style-type: none; padding: 0;">
        """
        
        for category, count in analytics.get('jobs_by_category', {}).items():
            percentage = (count / analytics.get('total_jobs', 1)) * 100
            html_body += f"""
                        <li style="padding: 8px; margin: 5px 0; background-color: white; border-radius: 5px; border-left: 4px solid #007bff;">
                            <strong>{category.replace('_', ' ').title()}:</strong> {count} positions ({percentage:.1f}%)
                        </li>
            """
        
        html_body += """
                    </ul>
                    
                    <h3>Work Mode Distribution:</h3>
                    <ul style="list-style-type: none; padding: 0;">
        """
        
        for mode, count in analytics.get('jobs_by_work_mode', {}).items():
            percentage = (count / analytics.get('total_jobs', 1)) * 100
            color = {'Remote': '#28a745', 'Hybrid': '#ffc107', 'On-site': '#dc3545'}.get(mode, '#007bff')
            html_body += f"""
                        <li style="padding: 8px; margin: 5px 0; background-color: white; border-radius: 5px; border-left: 4px solid {color};">
                            <strong>{mode}:</strong> {count} positions ({percentage:.1f}%)
                        </li>
            """
        
        # Add footer
        html_body += f"""
                    </ul>
                </div>
                
                <div class="footer">
                    <p><strong>Data Jobs Intelligence Pipeline</strong></p>
                    <p>Automated Job Market Analysis | Built with ❤️ by Eman Elgamal</p>
                    <p>Next Report: Tomorrow at 9:00 AM</p>
                    <p style="font-size: 12px; color: #999;">
                        Data Source: Glassdoor, Indeed | Processing Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Attach HTML body
        msg.attach(MIMEText(html_body, 'html'))
        
        # Attach chart images
        for chart_name, chart_path in chart_files:
            if os.path.exists(chart_path):
                with open(chart_path, 'rb') as f:
                    img_data = f.read()
                    image = MIMEBase('image', 'png')
                    image.set_payload(img_data)
                    encoders.encode_base64(image)
                    image.add_header('Content-ID', f'<{chart_name.lower().replace(" ", "_")}>')
                    image.add_header('Content-Disposition', f'inline; filename="{chart_name}.png"')
                    msg.attach(image)
        
        # Send email
        print("📤 Sending enhanced dashboard email...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        
        print(f"✅ Enhanced dashboard email sent successfully to {recipient_email}")
        print("📊 Email includes embedded charts and interactive analytics!")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced email sending failed: {e}")
        return False

def load_latest_job_data():
    """Load the most recent job data"""
    
    csv_files = glob.glob("data/production_jobs_*.csv") + glob.glob("data/enhanced_jobs_*.csv")
    
    if csv_files:
        latest_file = max(csv_files, key=os.path.getctime)
        df = pd.read_csv(latest_file)
        return df.to_dict('records')
    
    return []

def generate_analytics_from_data(jobs_data):
    """Generate analytics from job data"""
    
    if not jobs_data:
        return {}
    
    df = pd.DataFrame(jobs_data)
    
    analytics = {
        'total_jobs': len(df),
        'jobs_by_category': df['category'].value_counts().to_dict(),
        'jobs_by_work_mode': df['work_mode'].value_counts().to_dict(),
        'jobs_by_experience': df['experience_level'].value_counts().to_dict(),
        'jobs_by_type': df['job_type'].value_counts().to_dict(),
        'top_companies': df[df['company'] != 'N/A']['company'].value_counts().head(10).to_dict(),
        'sources': df['source'].value_counts().to_dict()
    }
    
    return analytics

def run_dashboard_email_pipeline():
    """Run the complete pipeline with enhanced dashboard email"""
    
    print("🚀 ENHANCED DASHBOARD EMAIL PIPELINE")
    print("="*60)
    
    try:
        # Load latest job data
        print("📂 Loading latest job data...")
        jobs_data = load_latest_job_data()
        
        if not jobs_data:
            print("❌ No job data found!")
            return False
        
        print(f"✅ Loaded {len(jobs_data)} jobs")
        
        # Generate analytics
        print("📊 Generating analytics...")
        analytics = generate_analytics_from_data(jobs_data)
        
        # Send enhanced email with dashboard
        success = send_enhanced_dashboard_email(jobs_data, analytics)
        
        if success:
            print("\n🎉 ENHANCED DASHBOARD EMAIL SENT SUCCESSFULLY!")
            print("📧 Check your email for the beautiful dashboard report!")
            print("📊 Includes embedded charts and detailed analytics")
        else:
            print("\n❌ Enhanced email sending failed")
        
        return success
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return False

if __name__ == "__main__":
    run_dashboard_email_pipeline()