import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import pandas as pd
from typing import List, Dict

class ErrorEmailNotifier:
    def __init__(self, smtp_server: str, port: int, sender_email: str, sender_password: str, admin_emails: List[str]):
        """
        初始化邮件通知服务
        
        Args:
            smtp_server: SMTP服务器地址
            port: 端口号
            sender_email: 发件人邮箱
            sender_password: 发件人密码/授权码
            admin_emails: 管理员邮箱列表
        """
        self.smtp_server = smtp_server
        self.port = port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.admin_emails = admin_emails
        
    def _generate_html_table(self, error_data: List[Dict]) -> str:
        """生成HTML格式的表格[6,7](@ref)"""
        if not error_data:
            return "<p>无错误数据</p>"
        
        # 从第一个字典获取表头
        headers = list(error_data[0].keys())
        
        # 使用Pandas生成HTML表格[6](@ref)
        df = pd.DataFrame(error_data)
        html_table = df.to_html(index=False, classes='error-table', border=1)
        
        # 添加一些基本样式[7](@ref)
        styled_html = f"""
        <html>
        <head>
        <style>
            table.error-table {{
                border-collapse: collapse;
                width: 100%;
                margin: 10px 0;
                font-family: Arial, sans-serif;
            }}
            table.error-table th, table.error-table td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            table.error-table th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            table.error-table tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            table.error-table tr:hover {{
                background-color: #f1f1f1;
            }}
        </style>
        </head>
        <body>
            {html_table}
        </body>
        </html>
        """
        return styled_html
    
    def send_error_notification(self, service: str, error_message: List[Dict]) -> bool:

        """
        发送错误通知邮件
        
        Args:
            service: 服务名称
            error_message: 错误信息列表，每个字典的key作为表头
        
        Returns:
            bool: 发送是否成功
        """
        try:
            # 创建邮件消息[1,2](@ref)
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"【错误告警】{service}服务异常 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.admin_emails)
            
            # 生成HTML表格内容[6,7](@ref)
            html_content = self._generate_html_table(error_message)
            
            # 创建纯文本版本作为备选
            text_content = f"""
            服务异常告警
            服务名称: {service}
            发生时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            错误详情:
            {str(error_message)}
            """
            
            # 添加文本和HTML版本[6](@ref)
            part1 = MIMEText(text_content, 'plain', 'utf-8')
            part2 = MIMEText(html_content, 'html', 'utf-8')
            
            msg.attach(part1)
            msg.attach(part2)
            
            # 连接SMTP服务器并发送邮件 - 改进错误处理
            try:
                server = smtplib.SMTP_SSL(self.smtp_server, self.port)
                # server.set_debuglevel(1)  # 添加调试信息
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
                server.quit()  # 正确关闭连接
            except smtplib.SMTPServerDisconnected as e:
                print(f"SMTP服务器连接断开: {e}")
                return False
            except smtplib.SMTPRecipientsRefused as e:
                print(f"收件人地址被拒绝: {e}")
                return False
            except smtplib.SMTPAuthenticationError as e:
                print(f"SMTP认证失败: {e}")
                return False
            except smtplib.SMTPException as e:
                print(f"SMTP错误: {e}")
                return False
            except Exception as e:
                print(f"发送邮件时发生未知错误: {e}")
                # 尝试关闭连接（如果存在）
                try:
                    server.quit()
                except:
                    pass
                return False
            
            print(f"错误通知邮件发送成功 - 服务: {service}")
            return True
            
        except Exception as e:
            print(f"发送错误通知邮件失败: {e}")
            return False


def get_email_notifier():
    from dotenv import load_dotenv
    load_dotenv('163_email.env')

    print(f'os_info:{
        os.getenv('SMTP_SERVER'),
        os.getenv('SMTP_PORT'),
        os.getenv('SENDER_EMAIL'),
        os.getenv('SENDER_PASSWORD'),
        os.getenv('ADMIN_EMAILS')
    }')
    
    # 添加环境变量验证
    required_envs = ['SMTP_SERVER', 'SMTP_PORT', 'SENDER_EMAIL', 'SENDER_PASSWORD', 'ADMIN_EMAILS']
    for env in required_envs:
        if not os.getenv(env):
            raise ValueError(f"必需的环境变量 {env} 未设置")
    
    return ErrorEmailNotifier(
        smtp_server=os.getenv('SMTP_SERVER'),
        port=int(os.getenv('SMTP_PORT')),
        sender_email=os.getenv('SENDER_EMAIL'),
        sender_password=os.getenv('SENDER_PASSWORD'),
        admin_emails=os.getenv('ADMIN_EMAILS').split(',')
    )


if __name__ == "__main__":
    email_notifier = get_email_notifier()
    email_notifier.send_error_notification("测试服务", [{"错误时间": "2023-04-01 12:00:00", "错误信息": "测试错误信息"}])