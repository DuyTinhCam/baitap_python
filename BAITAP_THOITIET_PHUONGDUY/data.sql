USE [THAYCOP]
GO

/****** Object:  Table [dbo].[weather]    Script Date: 5/14/2024 9:04:36 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[weather](
	[city] [nchar](255) NULL,
	[date] [date] NULL,
	[temp] [float] NULL
) ON [PRIMARY]
GO


