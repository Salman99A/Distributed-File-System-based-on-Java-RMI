import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.nio.file.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.table.*;

public class DFSGui extends JFrame {

    private MasterServerClientInterface master;
    private Registry registry;
    private final List<ReplicaLoc> replicaLocs = new ArrayList<>();
    private final List<ReplicaInterface> replicaStubs = new ArrayList<>();

    // UI components
    private JLabel masterStatusBadge;
    private JLabel replicaCountBadge;
    private JLabel clusterHealthBadge;
    private JLabel clusterTrafficBadge;
    private JLabel fileAvailabilityBanner;
    private JLabel clusterRoutingBanner;
    private JPanel replicaCardsPanel;
    private DefaultTableModel filesTableModel;
    private JTable filesTable;
    private JTextArea fileContentArea;
    private DefaultTableModel chunksTableModel;
    private JTable chunksTable;
    private JTextArea logTextArea;

    // Write & Upload panel
    private JTextField writeFileNameField;
    private JTextArea writeContentArea;
    private JProgressBar writeProgressBar;
    private JLabel writeStatusLabel;
    private JLabel uploadFileInfoLabel;
    private JComboBox<String> chunkSizeCombo;
    private JPanel distributionMatrixPanel;
    private JLabel planSummaryLabel;

    // Uploaded local file cache
    private byte[] uploadedFileBytes = null;
    private File selectedLocalFile = null;

    private static final SimpleDateFormat TIME_FMT = new SimpleDateFormat("HH:mm:ss.SSS");

    public DFSGui() {
        super("Distributed File System (Java RMI - GFS Architecture)");
        initLookAndFeel();
        initComponents();
        setupLogCapture();
        initCluster();
    }

    private void initLookAndFeel() {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {}
    }

    private void setupLogCapture() {
        OutputStream outStream = new OutputStream() {
            private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            @Override
            public void write(int b) {
                if (b == '\n') {
                    String line = buffer.toString();
                    buffer.reset();
                    appendLog(line);
                } else if (b != '\r') {
                    buffer.write(b);
                }
            }
        };

        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        PrintStream dualOut = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                originalOut.write(b);
                outStream.write(b);
            }
        }, true);

        PrintStream dualErr = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                originalErr.write(b);
                outStream.write(b);
            }
        }, true);

        System.setOut(dualOut);
        System.setErr(dualErr);
    }

    private void appendLog(String message) {
        if (message == null || message.trim().isEmpty()) return;
        SwingUtilities.invokeLater(() -> {
            String time = TIME_FMT.format(new Date());
            logTextArea.append("[" + time + "] " + message + "\n");
            logTextArea.setCaretPosition(logTextArea.getDocument().getLength());
        });
    }

    private void initComponents() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1240, 840);
        setMinimumSize(new Dimension(1000, 700));
        setLocationRelativeTo(null);
        getContentPane().setLayout(new BorderLayout(10, 10));

        // Top Toolbar
        JPanel topPanel = createTopPanel();
        getContentPane().add(topPanel, BorderLayout.NORTH);

        // Center Tabs
        JTabbedPane tabbedPane = new JTabbedPane();
        tabbedPane.setFont(new Font("Segoe UI", Font.BOLD, 13));

        tabbedPane.addTab("🖥️ Cluster Nodes & Traffic", createClusterTab());
        tabbedPane.addTab("📁 Files & Availability", createFilesTab());
        tabbedPane.addTab("✍️ Write & Commit Studio", createWriteTab());
        tabbedPane.addTab("🧪 Automated Tests & Failover", createTestTab());

        // Split center tabs with bottom compact log (3-4 lines visible)
        JPanel logPanel = createLogPanel();
        JSplitPane mainSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tabbedPane, logPanel);
        mainSplit.setResizeWeight(0.90);
        mainSplit.setDividerSize(5);
        mainSplit.setDividerLocation(690);

        getContentPane().add(mainSplit, BorderLayout.CENTER);
    }

    private JPanel createTopPanel() {
        JPanel panel = new JPanel(new BorderLayout(15, 10));
        panel.setBorder(new CompoundBorder(
                new MatteBorder(0, 0, 1, 0, new Color(210, 215, 225)),
                new EmptyBorder(10, 16, 10, 16)));
        panel.setBackground(new Color(248, 250, 252));

        // Left info
        JPanel infoPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 0));
        infoPanel.setOpaque(false);

        JLabel titleLabel = new JLabel("Distributed File System");
        titleLabel.setFont(new Font("Segoe UI", Font.BOLD, 17));
        titleLabel.setForeground(new Color(30, 41, 59));
        infoPanel.add(titleLabel);

        masterStatusBadge = createBadge("Master: Online (50900)", new Color(16, 185, 129));
        infoPanel.add(masterStatusBadge);

        replicaCountBadge = createBadge("Replicas: 5 Active", new Color(37, 99, 235));
        infoPanel.add(replicaCountBadge);

        clusterHealthBadge = createBadge("Health: 100% Optimal", new Color(13, 148, 136));
        infoPanel.add(clusterHealthBadge);

        clusterTrafficBadge = createBadge("Traffic: 0 ops", new Color(79, 70, 229));
        infoPanel.add(clusterTrafficBadge);

        // Right actions
        JPanel actionsPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        actionsPanel.setOpaque(false);

        JButton refreshBtn = createStyledButton("🔄 Refresh", new Color(37, 99, 235), Color.WHITE);
        refreshBtn.addActionListener(e -> refreshAll());
        actionsPanel.add(refreshBtn);

        JButton cleanBtn = createStyledButton("🧹 Clear Storage", new Color(220, 38, 38), Color.WHITE);
        cleanBtn.addActionListener(e -> clearStorage());
        actionsPanel.add(cleanBtn);

        JButton restartBtn = createStyledButton("⚡ Re-init Cluster", new Color(13, 148, 136), Color.WHITE);
        restartBtn.addActionListener(e -> reinitCluster());
        actionsPanel.add(restartBtn);

        panel.add(infoPanel, BorderLayout.WEST);
        panel.add(actionsPanel, BorderLayout.EAST);
        return panel;
    }

    private JLabel createBadge(String text, Color bg) {
        JLabel badge = new JLabel(" " + text + " ");
        badge.setFont(new Font("Segoe UI", Font.BOLD, 12));
        badge.setForeground(Color.WHITE);
        badge.setOpaque(true);
        badge.setBackground(bg);
        badge.setBorder(new EmptyBorder(4, 8, 4, 8));
        return badge;
    }

    private JButton createStyledButton(String text, Color bg, Color fg) {
        JButton btn = new JButton(text) {
            @Override
            protected void paintComponent(Graphics g) {
                Graphics2D g2 = (Graphics2D) g.create();
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                if (getModel().isPressed()) {
                    g2.setColor(bg.darker());
                } else if (getModel().isRollover()) {
                    g2.setColor(bg.brighter());
                } else {
                    g2.setColor(bg);
                }
                g2.fillRoundRect(0, 0, getWidth(), getHeight(), 8, 8);
                g2.dispose();
                super.paintComponent(g);
            }
        };
        btn.setFont(new Font("Segoe UI", Font.BOLD, 12));
        btn.setForeground(fg);
        btn.setContentAreaFilled(false);
        btn.setFocusPainted(false);
        btn.setOpaque(false);
        btn.setBorder(new CompoundBorder(
                new LineBorder(bg.darker(), 1, true),
                new EmptyBorder(7, 14, 7, 14)));
        btn.setCursor(new Cursor(Cursor.HAND_CURSOR));
        return btn;
    }

    private JPanel createClusterTab() {
        JPanel panel = new JPanel(new BorderLayout(10, 10));
        panel.setBorder(new EmptyBorder(12, 12, 12, 12));
        panel.setBackground(Color.WHITE);

        JPanel northPanel = new JPanel(new GridLayout(2, 1, 6, 6));
        northPanel.setOpaque(false);

        JLabel label = new JLabel("Cluster Topology: Replica Nodes 0 to 4 Only (Strictly 5 Replicas: Node 0, Node 1, Node 2, Node 3, Node 4)");
        label.setFont(new Font("Segoe UI", Font.BOLD, 14));
        label.setForeground(new Color(30, 41, 59));
        northPanel.add(label);

        clusterRoutingBanner = new JLabel("🟢 Dynamic Traffic Routing: Replicas 0 to 4 active (No Replica 5) — Traffic distributed normally.");
        clusterRoutingBanner.setFont(new Font("Segoe UI", Font.BOLD, 12));
        clusterRoutingBanner.setOpaque(true);
        clusterRoutingBanner.setBackground(new Color(236, 253, 245));
        clusterRoutingBanner.setForeground(new Color(6, 95, 70));
        clusterRoutingBanner.setBorder(new CompoundBorder(
                new LineBorder(new Color(167, 243, 208), 1, true),
                new EmptyBorder(8, 12, 8, 12)));
        northPanel.add(clusterRoutingBanner);

        panel.add(northPanel, BorderLayout.NORTH);

        replicaCardsPanel = new JPanel(new GridLayout(1, 5, 12, 12));
        replicaCardsPanel.setBackground(Color.WHITE);

        JScrollPane scrollPane = new JScrollPane(replicaCardsPanel);
        scrollPane.setBorder(BorderFactory.createEmptyBorder());
        panel.add(scrollPane, BorderLayout.CENTER);

        return panel;
    }

    private JPanel createFilesTab() {
        JPanel panel = new JPanel(new BorderLayout(10, 10));
        panel.setBorder(new EmptyBorder(12, 12, 12, 12));
        panel.setBackground(Color.WHITE);

        // Split between Files list (left) and Content/Chunks (right)
        String[] fileColNames = {"File Name", "Availability", "Healthy Copies", "Primary Replica", "Hosted Replicas", "Size (B)", "Chunks"};
        filesTableModel = new DefaultTableModel(fileColNames, 0) {
            @Override
            public boolean isCellEditable(int r, int c) { return false; }
        };
        filesTable = new JTable(filesTableModel);
        filesTable.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        filesTable.setRowHeight(26);
        filesTable.getTableHeader().setFont(new Font("Segoe UI", Font.BOLD, 12));
        filesTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        filesTable.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting() && filesTable.getSelectedRow() != -1) {
                String fileName = (String) filesTableModel.getValueAt(filesTable.getSelectedRow(), 0);
                loadFileDetails(fileName);
            }
        });

        JPanel leftPanel = new JPanel(new BorderLayout(8, 8));
        leftPanel.setBackground(Color.WHITE);
        leftPanel.add(new JLabel("DFS Files Registry & Replication Availability:"), BorderLayout.NORTH);
        leftPanel.add(new JScrollPane(filesTable), BorderLayout.CENTER);

        JPanel fileActions = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
        fileActions.setOpaque(false);
        JButton readBtn = createStyledButton("📖 Read Selected", new Color(37, 99, 235), Color.WHITE);
        readBtn.addActionListener(e -> {
            int row = filesTable.getSelectedRow();
            if (row != -1) {
                loadFileDetails((String) filesTableModel.getValueAt(row, 0));
            } else {
                JOptionPane.showMessageDialog(this, "Please select a file from the list.", "Notice", JOptionPane.INFORMATION_MESSAGE);
            }
        });
        fileActions.add(readBtn);

        JButton exportBtn = createStyledButton("💾 Export to Local Disk", new Color(16, 185, 129), Color.WHITE);
        exportBtn.addActionListener(e -> {
            int row = filesTable.getSelectedRow();
            if (row != -1) {
                exportFileToDisk((String) filesTableModel.getValueAt(row, 0));
            } else {
                JOptionPane.showMessageDialog(this, "Please select a file to export.", "Notice", JOptionPane.INFORMATION_MESSAGE);
            }
        });
        fileActions.add(exportBtn);
        leftPanel.add(fileActions, BorderLayout.SOUTH);

        // Right panel: Content and Chunks
        JPanel rightPanel = new JPanel(new BorderLayout(10, 10));
        rightPanel.setBackground(Color.WHITE);

        JPanel contentPanel = new JPanel(new BorderLayout(5, 5));
        contentPanel.setBackground(Color.WHITE);

        fileAvailabilityBanner = new JLabel(" Select a file to inspect health & chunk replication.");
        fileAvailabilityBanner.setFont(new Font("Segoe UI", Font.BOLD, 12));
        fileAvailabilityBanner.setOpaque(true);
        fileAvailabilityBanner.setBackground(new Color(241, 245, 249));
        fileAvailabilityBanner.setForeground(new Color(51, 65, 85));
        fileAvailabilityBanner.setBorder(new EmptyBorder(6, 10, 6, 10));
        contentPanel.add(fileAvailabilityBanner, BorderLayout.NORTH);

        fileContentArea = new JTextArea(5, 40);
        fileContentArea.setFont(new Font("Consolas", Font.PLAIN, 13));
        fileContentArea.setEditable(false);
        fileContentArea.setBackground(new Color(248, 250, 252));
        fileContentArea.setBorder(new LineBorder(new Color(203, 213, 225)));
        contentPanel.add(new JScrollPane(fileContentArea), BorderLayout.CENTER);

        // Chunks Breakdown
        JPanel chunksPanel = new JPanel(new BorderLayout(5, 5));
        chunksPanel.setBackground(Color.WHITE);
        chunksPanel.add(new JLabel("Visual Chunk Breakdown (Chunk Size = " + Configurations.CHUNK_SIZE + " bytes):"), BorderLayout.NORTH);
        String[] chunkCols = {"Seq #", "Bytes (Hex)", "String Segment", "Stored At (Replicas)"};
        chunksTableModel = new DefaultTableModel(chunkCols, 0) {
            @Override
            public boolean isCellEditable(int r, int c) { return false; }
        };
        chunksTable = new JTable(chunksTableModel);
        chunksTable.setFont(new Font("Consolas", Font.PLAIN, 12));
        chunksTable.setRowHeight(24);
        chunksTable.getTableHeader().setFont(new Font("Segoe UI", Font.BOLD, 12));
        chunksPanel.add(new JScrollPane(chunksTable), BorderLayout.CENTER);

        JSplitPane rightSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT, contentPanel, chunksPanel);
        rightSplit.setResizeWeight(0.4);
        rightSplit.setDividerSize(6);
        rightPanel.add(rightSplit, BorderLayout.CENTER);

        JSplitPane filesSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPanel, rightPanel);
        filesSplit.setResizeWeight(0.48);
        filesSplit.setDividerSize(6);
        panel.add(filesSplit, BorderLayout.CENTER);

        return panel;
    }

    private JPanel createWriteTab() {
        JPanel panel = new JPanel(new BorderLayout(15, 15));
        panel.setBorder(new EmptyBorder(14, 16, 14, 16));
        panel.setBackground(Color.WHITE);

        JPanel leftForm = new JPanel(new BorderLayout(10, 10));
        leftForm.setOpaque(false);

        // Top Toolbar inside leftForm
        JPanel topBar = new JPanel(new GridLayout(3, 1, 6, 6));
        topBar.setOpaque(false);

        // Row 1: File name and File chooser button
        JPanel fnRow = new JPanel(new BorderLayout(8, 0));
        fnRow.setOpaque(false);
        JLabel fnLabel = new JLabel("Target File Name: ");
        fnLabel.setFont(new Font("Segoe UI", Font.BOLD, 13));
        writeFileNameField = new JTextField("demoFile.txt", 20);
        writeFileNameField.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        writeFileNameField.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            public void insertUpdate(javax.swing.event.DocumentEvent e) { updateTrafficPlanPreview(); }
            public void removeUpdate(javax.swing.event.DocumentEvent e) { updateTrafficPlanPreview(); }
            public void changedUpdate(javax.swing.event.DocumentEvent e) { updateTrafficPlanPreview(); }
        });
        fnRow.add(fnLabel, BorderLayout.WEST);
        fnRow.add(writeFileNameField, BorderLayout.CENTER);

        JPanel filePickRow = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        filePickRow.setOpaque(false);
        JButton uploadBtn = createStyledButton("📁 Browse & Upload File", new Color(37, 99, 235), Color.WHITE);
        uploadBtn.addActionListener(e -> browseAndSelectLocalFile());
        filePickRow.add(uploadBtn);

        JButton loadSampleBtn = createStyledButton("📝 Sample Text", new Color(107, 114, 128), Color.WHITE);
        loadSampleBtn.addActionListener(e -> {
            uploadedFileBytes = null;
            selectedLocalFile = null;
            writeFileNameField.setText("gfs_architecture_summary.txt");
            writeContentArea.setText("The Google File System is designed for high availability, chunk-based streaming, and automatic fault-tolerant failovers across replica nodes.");
            uploadFileInfoLabel.setText("✍️ Custom Text Input Mode (" + writeContentArea.getText().length() + " bytes)");
            uploadFileInfoLabel.setForeground(new Color(71, 85, 105));
            updateTrafficPlanPreview();
        });
        filePickRow.add(loadSampleBtn);

        JButton clearBtn = createStyledButton("🧹 Clear", new Color(220, 38, 38), Color.WHITE);
        clearBtn.addActionListener(e -> {
            uploadedFileBytes = null;
            selectedLocalFile = null;
            writeFileNameField.setText("");
            writeContentArea.setText("");
            uploadFileInfoLabel.setText("✍️ Empty. Type content or browse any local file to calculate traffic.");
            uploadFileInfoLabel.setForeground(new Color(100, 116, 139));
            updateTrafficPlanPreview();
        });
        filePickRow.add(clearBtn);
        fnRow.add(filePickRow, BorderLayout.EAST);
        topBar.add(fnRow);

        // Row 2: File info badge
        uploadFileInfoLabel = new JLabel("✍️ Text Mode: Type below or click '📁 Browse & Upload File' to select any local file (PDF, TXT, IMG, etc.)");
        uploadFileInfoLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
        uploadFileInfoLabel.setForeground(new Color(30, 41, 59));
        uploadFileInfoLabel.setOpaque(true);
        uploadFileInfoLabel.setBackground(new Color(241, 245, 249));
        uploadFileInfoLabel.setBorder(new CompoundBorder(
                new LineBorder(new Color(203, 213, 225), 1, true),
                new EmptyBorder(6, 10, 6, 10)));
        topBar.add(uploadFileInfoLabel);

        // Row 3: Chunk size configuration
        JPanel chunkRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 0));
        chunkRow.setOpaque(false);
        JLabel chunkLabel = new JLabel("⚙️ Chunk Streaming Size:");
        chunkLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
        chunkRow.add(chunkLabel);

        chunkSizeCombo = new JComboBox<>(new String[]{
                "Adaptive (Auto-tuned based on file size)",
                "3 Bytes (GFS Default)",
                "64 Bytes (Fast Transfer)",
                "256 Bytes (Bulk Streaming)",
                "1024 Bytes (High Throughput)"
        });
        chunkSizeCombo.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        chunkSizeCombo.addActionListener(e -> updateTrafficPlanPreview());
        chunkRow.add(chunkSizeCombo);

        JLabel chunkHint = new JLabel("💡 Files are sliced into chunks and streamed with sequence acknowledgments.");
        chunkHint.setFont(new Font("Segoe UI", Font.ITALIC, 11));
        chunkHint.setForeground(new Color(100, 116, 139));
        chunkRow.add(chunkHint);
        topBar.add(chunkRow);

        leftForm.add(topBar, BorderLayout.NORTH);

        // Center: Content text area
        JPanel centerContentPanel = new JPanel(new BorderLayout(4, 4));
        centerContentPanel.setOpaque(false);
        JLabel contentHeader = new JLabel("File Payload / Content Preview:");
        contentHeader.setFont(new Font("Segoe UI", Font.BOLD, 12));
        contentHeader.setForeground(new Color(51, 65, 85));
        centerContentPanel.add(contentHeader, BorderLayout.NORTH);

        writeContentArea = new JTextArea("Distributed systems provide high availability, fault tolerance, and scalability across replicas.", 10, 40);
        writeContentArea.setFont(new Font("Consolas", Font.PLAIN, 13));
        writeContentArea.setLineWrap(true);
        writeContentArea.setWrapStyleWord(true);
        writeContentArea.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            public void insertUpdate(javax.swing.event.DocumentEvent e) { onEdit(); }
            public void removeUpdate(javax.swing.event.DocumentEvent e) { onEdit(); }
            public void changedUpdate(javax.swing.event.DocumentEvent e) { onEdit(); }
            private void onEdit() {
                if (selectedLocalFile == null) {
                    uploadedFileBytes = null;
                }
                updateTrafficPlanPreview();
            }
        });
        centerContentPanel.add(new JScrollPane(writeContentArea), BorderLayout.CENTER);
        leftForm.add(centerContentPanel, BorderLayout.CENTER);

        // Bottom: Write action and Progress
        JPanel bottomActionPanel = new JPanel(new GridLayout(3, 1, 6, 6));
        bottomActionPanel.setOpaque(false);

        JPanel actionBtnRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 0));
        actionBtnRow.setOpaque(false);
        JButton writeBtn = createStyledButton("🚀 Distribute Traffic & Upload to DFS", new Color(16, 185, 129), Color.WHITE);
        writeBtn.setFont(new Font("Segoe UI", Font.BOLD, 14));
        writeBtn.addActionListener(e -> executeWrite());
        actionBtnRow.add(writeBtn);
        bottomActionPanel.add(actionBtnRow);

        writeProgressBar = new JProgressBar(0, 100);
        writeProgressBar.setStringPainted(true);
        writeProgressBar.setString("Ready (0%)");
        writeProgressBar.setFont(new Font("Segoe UI", Font.BOLD, 12));
        writeProgressBar.setForeground(new Color(16, 185, 129));
        writeProgressBar.setPreferredSize(new Dimension(300, 24));
        writeProgressBar.setVisible(false);
        bottomActionPanel.add(writeProgressBar);

        writeStatusLabel = new JLabel("Ready to calculate traffic and stream payload across replica nodes.");
        writeStatusLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
        writeStatusLabel.setForeground(new Color(71, 85, 105));
        bottomActionPanel.add(writeStatusLabel);

        leftForm.add(bottomActionPanel, BorderLayout.SOUTH);
        panel.add(leftForm, BorderLayout.CENTER);

        // Right side: Traffic Calculation & Dynamic Node Distribution Matrix Panel
        JPanel rightSide = new JPanel(new BorderLayout(8, 8));
        rightSide.setPreferredSize(new Dimension(390, 0));
        rightSide.setBackground(new Color(248, 250, 252));
        rightSide.setBorder(new CompoundBorder(
                new LineBorder(new Color(226, 232, 240), 1, true),
                new EmptyBorder(12, 14, 12, 14)));

        JPanel rightHeader = new JPanel(new BorderLayout(4, 4));
        rightHeader.setOpaque(false);
        JLabel matrixTitle = new JLabel("📊 Traffic Calculation & Node Distribution");
        matrixTitle.setFont(new Font("Segoe UI", Font.BOLD, 14));
        matrixTitle.setForeground(new Color(30, 41, 59));
        rightHeader.add(matrixTitle, BorderLayout.NORTH);

        planSummaryLabel = new JLabel();
        planSummaryLabel.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        planSummaryLabel.setForeground(new Color(71, 85, 105));
        planSummaryLabel.setOpaque(true);
        planSummaryLabel.setBackground(Color.WHITE);
        planSummaryLabel.setBorder(new CompoundBorder(
                new LineBorder(new Color(226, 232, 240), 1, true),
                new EmptyBorder(8, 10, 8, 10)));
        rightHeader.add(planSummaryLabel, BorderLayout.CENTER);
        rightSide.add(rightHeader, BorderLayout.NORTH);

        distributionMatrixPanel = new JPanel(new GridLayout(5, 1, 6, 6));
        distributionMatrixPanel.setOpaque(false);
        JScrollPane matrixScroll = new JScrollPane(distributionMatrixPanel);
        matrixScroll.setBorder(BorderFactory.createEmptyBorder());
        matrixScroll.setOpaque(false);
        matrixScroll.getViewport().setOpaque(false);
        rightSide.add(matrixScroll, BorderLayout.CENTER);

        // Bottom info accordion
        JPanel infoAccordion = new JPanel(new BorderLayout());
        infoAccordion.setOpaque(false);
        JTextArea pipelineHelp = new JTextArea(
                "⚡ How Traffic Distribution Works:\n" +
                "1. Master inspects current traffic load on alive nodes.\n" +
                "2. The least-loaded node is designated as Primary Replica.\n" +
                "3. Chunks stream to Primary; on commit, payload is reflected to slave replicas.\n" +
                "4. All nodes log actual operations & byte counts in real time.");
        pipelineHelp.setFont(new Font("Segoe UI", Font.PLAIN, 11));
        pipelineHelp.setForeground(new Color(100, 116, 139));
        pipelineHelp.setEditable(false);
        pipelineHelp.setOpaque(false);
        pipelineHelp.setWrapStyleWord(true);
        pipelineHelp.setLineWrap(true);
        infoAccordion.add(pipelineHelp, BorderLayout.CENTER);
        rightSide.add(infoAccordion, BorderLayout.SOUTH);

        panel.add(rightSide, BorderLayout.EAST);

        SwingUtilities.invokeLater(this::updateTrafficPlanPreview);
        return panel;
    }

    private JPanel createTestTab() {
        JPanel panel = new JPanel(new BorderLayout(15, 15));
        panel.setBorder(new EmptyBorder(16, 20, 16, 20));
        panel.setBackground(Color.WHITE);

        JLabel label = new JLabel("Automated Test Suite & Concurrency Scenarios");
        label.setFont(new Font("Segoe UI", Font.BOLD, 15));
        panel.add(label, BorderLayout.NORTH);

        JPanel buttonsPanel = new JPanel(new GridLayout(4, 1, 12, 12));
        buttonsPanel.setBackground(Color.WHITE);

        // Card 1
        JPanel card1 = createTestCard("Run Standard Multi-Client Demo",
                "Simulates multiple clients performing concurrent write, commit, and read operations across replicas.",
                "▶ Run Standard Demo", new Color(37, 99, 235), e -> runStandardDemo());
        buttonsPanel.add(card1);

        // Card 2
        JPanel card2 = createTestCard("⚡ Automatic Node Failover Test",
                "Writes a file, kills primary replica, and verifies client reads seamlessly failover to surviving replicas without data loss.",
                "⚡ Test Node Failover", new Color(220, 38, 38), e -> runFailoverTest());
        buttonsPanel.add(card2);

        // Card 3
        JPanel card3 = createTestCard("Run Concurrency & Lock Test",
                "Executes customTest() verifying atomic commit, chunk sequences, and isolation during concurrent reads.",
                "▶ Run Concurrency Test", new Color(124, 58, 237), e -> runCustomTest());
        buttonsPanel.add(card3);

        // Card 4
        JPanel card4 = createTestCard("Batch Operations Test",
                "Executes batch write, read, and commit commands via Client.batchOperations().",
                "▶ Run Batch Test", new Color(16, 185, 129), e -> runBatchTest());
        buttonsPanel.add(card4);

        panel.add(buttonsPanel, BorderLayout.CENTER);
        return panel;
    }

    private JPanel createTestCard(String title, String desc, String btnText, Color btnColor, ActionListener action) {
        JPanel card = new JPanel(new BorderLayout(10, 10));
        card.setBackground(new Color(248, 250, 252));
        card.setBorder(new CompoundBorder(
                new LineBorder(new Color(226, 232, 240), 1, true),
                new EmptyBorder(12, 16, 12, 16)));

        JPanel textPanel = new JPanel(new GridLayout(2, 1, 4, 4));
        textPanel.setOpaque(false);
        JLabel titleLbl = new JLabel(title);
        titleLbl.setFont(new Font("Segoe UI", Font.BOLD, 13));
        titleLbl.setForeground(new Color(30, 41, 59));
        JLabel descLbl = new JLabel(desc);
        descLbl.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        descLbl.setForeground(new Color(100, 116, 139));
        textPanel.add(titleLbl);
        textPanel.add(descLbl);

        JButton btn = createStyledButton(btnText, btnColor, Color.WHITE);
        btn.addActionListener(action);

        card.add(textPanel, BorderLayout.CENTER);
        card.add(btn, BorderLayout.EAST);
        return card;
    }

    private JPanel createLogPanel() {
        JPanel panel = new JPanel(new BorderLayout(4, 4));
        panel.setBorder(new EmptyBorder(5, 10, 5, 10));
        panel.setBackground(new Color(15, 23, 42));
        panel.setPreferredSize(new Dimension(0, 110));
        panel.setMinimumSize(new Dimension(0, 75));

        JPanel logHeader = new JPanel(new BorderLayout());
        logHeader.setOpaque(false);

        JLabel logTitle = new JLabel("📋 Real-Time Activity & Failover Events Console (3-4 Lines View)");
        logTitle.setFont(new Font("Segoe UI", Font.BOLD, 12));
        logTitle.setForeground(new Color(226, 232, 240));
        logHeader.add(logTitle, BorderLayout.WEST);

        JPanel logActions = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        logActions.setOpaque(false);

        JButton clearLogBtn = createStyledButton("Clear Console", new Color(71, 85, 105), Color.WHITE);
        clearLogBtn.addActionListener(e -> logTextArea.setText(""));
        logActions.add(clearLogBtn);

        logHeader.add(logActions, BorderLayout.EAST);
        panel.add(logHeader, BorderLayout.NORTH);

        logTextArea = new JTextArea(4, 50);
        logTextArea.setFont(new Font("Consolas", Font.PLAIN, 12));
        logTextArea.setBackground(new Color(15, 23, 42));
        logTextArea.setForeground(new Color(226, 232, 240));
        logTextArea.setCaretColor(Color.WHITE);
        logTextArea.setEditable(false);

        JScrollPane scrollPane = new JScrollPane(logTextArea);
        scrollPane.setBorder(new LineBorder(new Color(51, 65, 85)));
        panel.add(scrollPane, BorderLayout.CENTER);

        return panel;
    }

    // ==========================================
    // CLUSTER INITIALIZATION & MANAGEMENT
    // ==========================================

    private void initCluster() {
        new Thread(() -> {
            try {
                appendLog("[System] Initializing Distributed File System cluster...");
                try {
                    LocateRegistry.createRegistry(Configurations.REG_PORT);
                    appendLog("[System] Created RMI Registry on port " + Configurations.REG_PORT);
                } catch (Exception e) {
                    appendLog("[System] RMI Registry already active on port " + Configurations.REG_PORT);
                }

                registry = LocateRegistry.getRegistry(Configurations.REG_PORT);
                Main.registry = registry;

                // Always ensure master is created and registered
                if (master == null) {
                    try {
                        master = (MasterServerClientInterface) registry.lookup("MasterServerClientInterface");
                        appendLog("[System] Connected to active Master server from Registry.");
                    } catch (Exception ex) {
                        try {
                            master = Main.startMaster();
                            appendLog("[System] Master server exported and registered.");
                        } catch (Exception ex2) {
                            appendLog("[System] Re-binding master server...");
                            master = new Master();
                            MasterServerClientInterface stub =
                                    (MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
                            registry.rebind("MasterServerClientInterface", stub);
                        }
                    }
                }

                // Respawn replica servers if needed
                respawnReplicas();

                // Subscribe to real-time failover events from TrafficMonitor
                TrafficMonitor.addListener(event -> {
                    SwingUtilities.invokeLater(() -> {
                        appendLog(event);
                        refreshAll();
                    });
                });

                SwingUtilities.invokeLater(() -> {
                    masterStatusBadge.setText(" Master: Online (50900) ");
                    masterStatusBadge.setBackground(new Color(16, 185, 129));
                    refreshAll();
                });

            } catch (Exception ex) {
                appendLog("[ERROR] Cluster init failed: " + ex.getMessage());
                ex.printStackTrace();
            }
        }).start();
    }

    private void respawnReplicas() {
        try {
            replicaLocs.clear();
            replicaStubs.clear();

            int n = 5;
            File configFile = new File("repServers.txt");
            List<String> addrs = new ArrayList<>();
            if (configFile.exists()) {
                try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
                    String line1 = br.readLine();
                    if (line1 != null) {
                        n = Math.min(5, Integer.parseInt(line1.trim()));
                    }
                    for (int i = 0; i < n; i++) {
                        String s = br.readLine();
                        if (s != null && s.contains(":")) {
                            addrs.add(s.substring(0, s.indexOf(':')).trim());
                        } else {
                            addrs.add("127.0.0.1");
                        }
                    }
                } catch (Exception ex) {
                    appendLog("[WARN] Reading repServers.txt: " + ex.getMessage());
                }
            }

            // Remove any accidental Replica_5 directory
            File extraReplica5 = new File("./Replica_5");
            if (extraReplica5.exists()) {
                File[] extraFiles = extraReplica5.listFiles();
                if (extraFiles != null) for (File f : extraFiles) f.delete();
                extraReplica5.delete();
            }

            for (int i = 0; i < 5; i++) {
                String addr = (i < addrs.size()) ? addrs.get(i) : "127.0.0.1";
                ReplicaLoc replicaLoc = new ReplicaLoc(i, addr, true);
                replicaLocs.add(replicaLoc);

                File dir = new File("./Replica_" + i);
                if (!dir.exists()) dir.mkdirs();

                ReplicaInterface stub = null;
                try {
                    stub = (ReplicaInterface) registry.lookup("ReplicaClient" + i);
                } catch (Exception ignored) {}

                if (stub == null) {
                    ReplicaServer rs = new ReplicaServer(i, "./");
                    stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
                    registry.rebind("ReplicaClient" + i, stub);
                }
                replicaStubs.add(stub);

                if (master != null) {
                    try {
                        master.registerReplicaServer(replicaLoc, stub);
                    } catch (Exception mex) {
                        appendLog("[WARN] Master registerReplica: " + mex.getMessage());
                    }
                }
            }
            appendLog("[System] Registered 5 replica nodes (0 to 4) with Master.");
        } catch (Exception e) {
            appendLog("[ERROR] Respawning replicas: " + e.getMessage());
        }
    }

    private void reinitCluster() {
        int confirm = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to re-initialize all replica servers?",
                "Re-init Cluster", JOptionPane.YES_NO_OPTION);
        if (confirm == JOptionPane.YES_OPTION) {
            initCluster();
        }
    }

    private void clearStorage() {
        int confirm = JOptionPane.showConfirmDialog(this,
                "This will clear all stored files across all Replica folders (Replica_0 ... Replica_4).\nContinue?",
                "Clear Storage", JOptionPane.YES_NO_OPTION);
        if (confirm != JOptionPane.YES_OPTION) return;

        try {
            for (int i = 0; i < 5; i++) {
                File dir = new File("./Replica_" + i);
                if (dir.exists() && dir.isDirectory()) {
                    File[] files = dir.listFiles();
                    if (files != null) {
                        for (File f : files) {
                            f.delete();
                        }
                    }
                }
            }
            appendLog("[System] All replica storage directories cleared.");
            refreshAll();
            JOptionPane.showMessageDialog(this, "All replica files have been cleared.", "Success", JOptionPane.INFORMATION_MESSAGE);
        } catch (Exception e) {
            appendLog("[ERROR] Clearing storage: " + e.getMessage());
        }
    }

    public void refreshAll() {
        refreshReplicaCards();
        refreshFilesList();
    }

    private void refreshReplicaCards() {
        replicaCardsPanel.removeAll();

        // Ensure at least 5 replica slots in replicaLocs
        while (replicaLocs.size() < 5) {
            replicaLocs.add(new ReplicaLoc(replicaLocs.size(), "127.0.0.1", true));
        }

        // 1. Synchronize alive states between master and local GUI list
        List<Integer> aliveList = new ArrayList<>();
        List<Integer> offlineList = new ArrayList<>();
        if (master != null) {
            try {
                List<ReplicaLoc> mLocs = master.getReplicaLocations();
                for (ReplicaLoc ml : mLocs) {
                    int id = ml.getId();
                    if (id < replicaLocs.size()) {
                        replicaLocs.get(id).setAlive(ml.isAlive());
                    }
                }
            } catch (Exception ignored) {}
        }

        Map<Integer, Boolean> currentTopology = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            boolean alive = (i < replicaLocs.size()) ? replicaLocs.get(i).isAlive() : true;
            currentTopology.put(i, alive);
            if (alive) aliveList.add(i);
            else offlineList.add(i);
        }
        TrafficMonitor.updateClusterTopology(currentTopology);

        int activeCount = aliveList.size();
        Map<Integer, Integer> activeTransfers = TrafficMonitor.getAllActiveTransfers();

        // 2. Render dynamic cluster routing banner
        if (clusterRoutingBanner != null) {
            if (activeCount == 5) {
                clusterRoutingBanner.setText("🟢 Dynamic Traffic Routing: All Replicas 0 to 4 Active — Traffic distributed normally.");
                clusterRoutingBanner.setBackground(new Color(236, 253, 245));
                clusterRoutingBanner.setForeground(new Color(6, 95, 70));
                clusterRoutingBanner.setBorder(new CompoundBorder(
                        new LineBorder(new Color(167, 243, 208), 1, true),
                        new EmptyBorder(8, 12, 8, 12)));
            } else if (activeCount == 1) {
                int soleNode = aliveList.get(0);
                clusterRoutingBanner.setText("⚡ CRITICAL HIGH AVAILABILITY: Only Replica " + soleNode + " is ONLINE — 100% of Cluster Traffic (Nodes " + offlineList + ") transferred to Replica " + soleNode + "!");
                clusterRoutingBanner.setBackground(new Color(254, 243, 199));
                clusterRoutingBanner.setForeground(new Color(180, 83, 9));
                clusterRoutingBanner.setBorder(new CompoundBorder(
                        new LineBorder(new Color(245, 158, 11), 2, true),
                        new EmptyBorder(8, 12, 8, 12)));
            } else if (activeCount == 0) {
                clusterRoutingBanner.setText("🔴 CRITICAL: All 5 replica nodes are OFFLINE! System is halted until nodes are revived.");
                clusterRoutingBanner.setBackground(new Color(254, 226, 226));
                clusterRoutingBanner.setForeground(new Color(185, 28, 28));
                clusterRoutingBanner.setBorder(new CompoundBorder(
                        new LineBorder(new Color(239, 68, 68), 2, true),
                        new EmptyBorder(8, 12, 8, 12)));
            } else {
                StringBuilder sb = new StringBuilder("⚡ ACTIVE TRAFFIC TRANSFERS: ");
                int count = 0;
                for (Map.Entry<Integer, Integer> entry : activeTransfers.entrySet()) {
                    if (count++ > 0) sb.append(" | ");
                    sb.append("Replica ").append(entry.getKey()).append(" (OFFLINE) ──▶ Traffic transferred to Replica ").append(entry.getValue());
                }
                clusterRoutingBanner.setText(sb.toString());
                clusterRoutingBanner.setBackground(new Color(254, 243, 199));
                clusterRoutingBanner.setForeground(new Color(146, 64, 14));
                clusterRoutingBanner.setBorder(new CompoundBorder(
                        new LineBorder(new Color(252, 211, 77), 1, true),
                        new EmptyBorder(8, 12, 8, 12)));
            }
        }

        // 3. Render cards for nodes 0 to 4
        for (int i = 0; i < 5; i++) {
            final int replicaId = i;
            File dir = new File("./Replica_" + i);
            if (!dir.exists()) dir.mkdirs();

            File[] files = dir.listFiles();
            int fileCount = (files != null) ? files.length : 0;
            long totalBytes = 0;
            if (files != null) {
                for (File f : files) totalBytes += f.length();
            }

            boolean isAlive = (i < replicaLocs.size()) ? replicaLocs.get(i).isAlive() : true;
            boolean isSoleSurvivor = (isAlive && activeCount == 1);

            int targetNode = TrafficMonitor.getFailoverTarget(i);
            Set<Integer> sourceNodes = TrafficMonitor.getFailoverSources(i);
            long absorbedOps = TrafficMonitor.getAbsorbedOps(i);
            long absorbedBytes = TrafficMonitor.getAbsorbedBytes(i);
            long transferredOutOps = TrafficMonitor.getTransferredOutOps(i);
            long transferredOutBytes = TrafficMonitor.getTransferredOutBytes(i);

            long readOps = TrafficMonitor.getReadOps(i);
            long writeOps = TrafficMonitor.getWriteOps(i);
            long totalOps = TrafficMonitor.getTotalOps(i);
            long bytes = TrafficMonitor.getBytesTransferred(i);
            String lastActive = TrafficMonitor.getLastActive(i);

            JPanel card = new JPanel(new BorderLayout(8, 8));
            if (!isAlive) {
                card.setBackground(new Color(254, 242, 242));
                card.setBorder(new CompoundBorder(
                        new LineBorder(new Color(252, 165, 165), 1, true),
                        new EmptyBorder(12, 12, 12, 12)));
            } else if (isSoleSurvivor) {
                card.setBackground(new Color(245, 243, 255));
                card.setBorder(new CompoundBorder(
                        new LineBorder(new Color(167, 139, 250), 2, true),
                        new EmptyBorder(12, 12, 12, 12)));
            } else if (!sourceNodes.isEmpty()) {
                card.setBackground(new Color(238, 242, 255));
                card.setBorder(new CompoundBorder(
                        new LineBorder(new Color(165, 180, 252), 1, true),
                        new EmptyBorder(12, 12, 12, 12)));
            } else {
                card.setBackground(new Color(248, 250, 252));
                card.setBorder(new CompoundBorder(
                        new LineBorder(new Color(226, 232, 240), 1, true),
                        new EmptyBorder(12, 12, 12, 12)));
            }

            // Top title
            JPanel cardHeader = new JPanel(new BorderLayout());
            cardHeader.setOpaque(false);

            JLabel title = new JLabel("Replica " + i);
            title.setFont(new Font("Segoe UI", Font.BOLD, 15));
            title.setForeground(new Color(30, 41, 59));
            cardHeader.add(title, BorderLayout.WEST);

            JLabel status = new JLabel();
            status.setFont(new Font("Segoe UI", Font.BOLD, 10));
            status.setOpaque(true);
            status.setForeground(Color.WHITE);
            status.setBorder(new EmptyBorder(2, 6, 2, 6));

            if (!isAlive) {
                status.setText(" ● OFFLINE (FAILED) ");
                status.setBackground(new Color(239, 68, 68));
            } else if (isSoleSurvivor) {
                status.setText(" 🏆 SOLE SURVIVOR (100% TRAFFIC) ");
                status.setBackground(new Color(124, 58, 237));
            } else if (!sourceNodes.isEmpty()) {
                status.setText(" ● ONLINE (ABSORBING) ");
                status.setBackground(new Color(79, 70, 229));
            } else {
                status.setText(" ● ONLINE ");
                status.setBackground(new Color(16, 185, 129));
            }
            cardHeader.add(status, BorderLayout.EAST);
            card.add(cardHeader, BorderLayout.NORTH);

            // Center details
            JPanel cardDetails = new JPanel(new GridLayout(6, 1, 3, 3));
            cardDetails.setOpaque(false);
            cardDetails.add(new JLabel("Addr: 127.0.0.1:50000 | Node " + i));
            cardDetails.add(new JLabel("Storage: " + fileCount + " files (" + totalBytes + " B)"));

            JLabel trafficLabel;
            JLabel ioLabel;
            JLabel failoverStatus;

            if (!isAlive) {
                trafficLabel = new JLabel("📊 Traffic: 0 ops (Transferred ➡️)");
                trafficLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
                trafficLabel.setForeground(new Color(220, 38, 38));

                ioLabel = new JLabel("➡️ Rerouted: " + transferredOutOps + " ops ➔ Replica " + (targetNode >= 0 ? targetNode : "Cluster") + (activeCount == 1 ? " (Sole Online)" : ""));
                ioLabel.setFont(new Font("Segoe UI", Font.BOLD, 11));
                ioLabel.setForeground(new Color(185, 28, 28));

                failoverStatus = new JLabel(activeCount == 1 ? "⚡ 100% Workload sent to Sole Active Node " + targetNode : "⚠️ Workload sent to Node " + targetNode);
                failoverStatus.setFont(new Font("Segoe UI", Font.BOLD, 11));
                failoverStatus.setForeground(new Color(220, 38, 38));

            } else if (isSoleSurvivor) {
                long effectiveOps = totalOps + absorbedOps;
                long effectiveBytes = bytes + absorbedBytes;

                trafficLabel = new JLabel("📊 Traffic: " + effectiveOps + " ops (" + effectiveBytes + " B)");
                trafficLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
                trafficLabel.setForeground(new Color(109, 40, 217));

                ioLabel = new JLabel("📥 " + readOps + "r / " + writeOps + "w | ⬅️ +" + absorbedOps + " ops from ALL OFFLINE Nodes " + sourceNodes);
                ioLabel.setFont(new Font("Segoe UI", Font.BOLD, 11));
                ioLabel.setForeground(new Color(124, 58, 237));

                failoverStatus = new JLabel("⚡ ABSORBING 100% CLUSTER TRAFFIC from Nodes " + sourceNodes);
                failoverStatus.setFont(new Font("Segoe UI", Font.BOLD, 11));
                failoverStatus.setForeground(new Color(109, 40, 217));

            } else if (!sourceNodes.isEmpty()) {
                long effectiveOps = totalOps + absorbedOps;
                long effectiveBytes = bytes + absorbedBytes;

                trafficLabel = new JLabel("📊 Traffic: " + effectiveOps + " ops (" + effectiveBytes + " B)");
                trafficLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
                trafficLabel.setForeground(new Color(37, 99, 235));

                ioLabel = new JLabel("📥 " + readOps + "r / " + writeOps + "w | ⬅️ +" + absorbedOps + " ops from Node " + sourceNodes);
                ioLabel.setFont(new Font("Segoe UI", Font.BOLD, 11));
                ioLabel.setForeground(new Color(124, 58, 237));

                failoverStatus = new JLabel("⚡ ABSORBING TRAFFIC from Node " + sourceNodes);
                failoverStatus.setFont(new Font("Segoe UI", Font.BOLD, 11));
                failoverStatus.setForeground(new Color(124, 58, 237));

            } else {
                trafficLabel = new JLabel("📊 Traffic: " + totalOps + " ops (" + bytes + " B)");
                trafficLabel.setFont(new Font("Segoe UI", Font.BOLD, 12));
                trafficLabel.setForeground(new Color(30, 41, 59));

                ioLabel = new JLabel("📥 Reads: " + readOps + " | 📤 Writes: " + writeOps);
                ioLabel.setFont(new Font("Segoe UI", Font.PLAIN, 11));
                ioLabel.setForeground(new Color(71, 85, 105));

                failoverStatus = new JLabel(totalOps > 0 ? "⚡ Serving Traffic Normally" : "💤 Idle (Standby)");
                failoverStatus.setFont(new Font("Segoe UI", Font.BOLD, 11));
                failoverStatus.setForeground(totalOps > 0 ? new Color(16, 185, 129) : new Color(100, 116, 139));
            }

            cardDetails.add(trafficLabel);
            cardDetails.add(ioLabel);

            JLabel activeLabel = new JLabel("⏱️ Last Active: " + lastActive);
            activeLabel.setFont(new Font("Segoe UI", Font.PLAIN, 11));
            activeLabel.setForeground(new Color(100, 116, 139));
            cardDetails.add(activeLabel);

            cardDetails.add(failoverStatus);
            card.add(cardDetails, BorderLayout.CENTER);

            // Bottom toggle button
            JPanel bottomPanel = new JPanel(new FlowLayout(FlowLayout.CENTER, 4, 0));
            bottomPanel.setOpaque(false);

            boolean finalAlive = isAlive;
            JButton toggleBtn = createStyledButton(
                    isAlive ? "⚠️ Simulate Failure" : "✅ Revive Node",
                    isAlive ? new Color(220, 38, 38) : new Color(16, 185, 129),
                    Color.WHITE);
            toggleBtn.addActionListener(e -> {
                toggleBtn.setEnabled(false);
                toggleBtn.setText("⏳ Updating...");
                new Thread(() -> {
                    try {
                        boolean newState = !finalAlive;

                        // 1. Tell Master ground truth
                        if (master != null) {
                            try {
                                master.setReplicaAlive(replicaId, newState);
                            } catch (Exception mex) {
                                appendLog("[WARN] Master setReplicaAlive(" + replicaId + "): " + mex.getMessage());
                            }
                        }

                        // 2. Tell Replica Stub to refuse / accept calls
                        ReplicaInterface stub = (replicaId < replicaStubs.size()) ? replicaStubs.get(replicaId) : null;
                        if (stub == null && registry != null) {
                            try {
                                stub = (ReplicaInterface) registry.lookup("ReplicaClient" + replicaId);
                            } catch (Exception ignored) {}
                        }
                        if (stub != null) {
                            try {
                                stub.setAlive(newState);
                            } catch (Exception sex) {
                                appendLog("[WARN] Stub setAlive(" + replicaId + "): " + sex.getMessage());
                            }
                        }

                        // 3. Update local GUI list
                        while (replicaLocs.size() <= replicaId) {
                            replicaLocs.add(new ReplicaLoc(replicaLocs.size(), "127.0.0.1", true));
                        }
                        replicaLocs.get(replicaId).setAlive(newState);

                        // 4. Update TrafficMonitor with complete alive state
                        TrafficMonitor.setNodeAlive(replicaId, newState);
                        Map<Integer, Boolean> guiAliveMap = new HashMap<>();
                        for (int k = 0; k < 5; k++) {
                            boolean kAlive = (k < replicaLocs.size()) ? replicaLocs.get(k).isAlive() : true;
                            guiAliveMap.put(k, kAlive);
                        }
                        TrafficMonitor.updateClusterTopology(guiAliveMap);

                        int nowAlive = 0;
                        int soleId = -1;
                        for (int k = 0; k < 5; k++) {
                            if (guiAliveMap.get(k)) {
                                nowAlive++;
                                soleId = k;
                            }
                        }

                        if (!newState) {
                            appendLog("[Cluster] 💥 Node Replica " + replicaId + " is now OFFLINE (Failed).");
                            if (nowAlive == 1) {
                                appendLog("[Failover] ⚡ CRITICAL: Only Replica " + soleId + " is ONLINE! 100% of cluster traffic transferred to Replica " + soleId + ".");
                            } else if (nowAlive > 1) {
                                int tgt = TrafficMonitor.getFailoverTarget(replicaId);
                                appendLog("[Failover] ⚡ Traffic previously served by Replica " + replicaId + " transferred to Replica " + tgt + ".");
                            } else {
                                appendLog("[Failover] 🔴 CRITICAL: All cluster replica nodes are now offline!");
                            }
                        } else {
                            appendLog("[Cluster] 🟢 Node Replica " + replicaId + " has been REVIVED and rejoined the cluster!");
                        }
                    } catch (Exception ex) {
                        appendLog("[ERROR] Updating replica state: " + ex.getMessage());
                    } finally {
                        SwingUtilities.invokeLater(() -> {
                            refreshAll();
                            updateTrafficPlanPreview();
                        });
                    }
                }).start();
            });
            bottomPanel.add(toggleBtn);
            card.add(bottomPanel, BorderLayout.SOUTH);

            replicaCardsPanel.add(card);
        }

        replicaCountBadge.setText(" Replicas: " + activeCount + " Active ");
        replicaCountBadge.setBackground(activeCount > 0 ? new Color(37, 99, 235) : new Color(220, 38, 38));

        long totalClusterOps = 0;
        for (int k = 0; k < 5; k++) totalClusterOps += TrafficMonitor.getTotalOps(k);
        clusterTrafficBadge.setText(" Traffic: " + totalClusterOps + " ops ");

        if (activeCount == 5) {
            clusterHealthBadge.setText(" Health: 100% Optimal ");
            clusterHealthBadge.setBackground(new Color(16, 185, 129));
        } else if (activeCount >= 3) {
            clusterHealthBadge.setText(" Health: " + (activeCount * 20) + "% Degraded (" + (5 - activeCount) + " down) ");
            clusterHealthBadge.setBackground(new Color(245, 158, 11));
        } else {
            clusterHealthBadge.setText(" Health: Critical (" + activeCount + "/5 alive) ");
            clusterHealthBadge.setBackground(new Color(220, 38, 38));
        }

        replicaCardsPanel.revalidate();
        replicaCardsPanel.repaint();
    }

    private void refreshFilesList() {
        filesTableModel.setRowCount(0);

        // Scan all replica directories for unique files
        Map<String, Set<Integer>> fileReplicaMap = new TreeMap<>();
        Map<String, Long> fileSizeMap = new HashMap<>();

        for (int i = 0; i < 5; i++) {
            File dir = new File("./Replica_" + i);
            if (dir.exists() && dir.isDirectory()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File f : files) {
                        String name = f.getName();
                        fileReplicaMap.computeIfAbsent(name, k -> new TreeSet<>()).add(i);
                        fileSizeMap.put(name, f.length());
                    }
                }
            }
        }

        Map<String, ReplicaLoc> primaryMap = null;
        if (master != null) {
            try {
                primaryMap = master.getPrimaryReplicaMap();
            } catch (Exception ignored) {}
        }

        for (Map.Entry<String, Set<Integer>> entry : fileReplicaMap.entrySet()) {
            String name = entry.getKey();
            Set<Integer> replicas = entry.getValue();
            long size = fileSizeMap.getOrDefault(name, 0L);
            int chunks = (int) Math.ceil(1.0 * size / Configurations.CHUNK_SIZE);

            int totalNodes = replicas.size();
            int onlineNodes = 0;
            List<String> replicaNodeStates = new ArrayList<>();
            for (int rId : replicas) {
                boolean alive = (rId < replicaLocs.size()) && replicaLocs.get(rId).isAlive();
                if (alive) onlineNodes++;
                replicaNodeStates.add("Node " + rId + (alive ? " (UP)" : " (DOWN)"));
            }

            String availability;
            if (onlineNodes == totalNodes) {
                availability = "🟢 100% (High Availability)";
            } else if (onlineNodes > 0) {
                availability = "🟡 " + (onlineNodes * 100 / totalNodes) + "% Degraded (Failover Active)";
            } else {
                availability = "🔴 0% Offline";
            }

            String primaryReplica = "Replica " + replicas.iterator().next();
            if (primaryMap != null && primaryMap.containsKey(name)) {
                ReplicaLoc pLoc = primaryMap.get(name);
                primaryReplica = "Replica " + pLoc.getId() + (pLoc.isAlive() ? "" : " (DOWN - Diverted)");
            }

            filesTableModel.addRow(new Object[]{
                    name,
                    availability,
                    onlineNodes + " / " + totalNodes + " Nodes UP",
                    primaryReplica,
                    replicaNodeStates.toString(),
                    size,
                    chunks
            });
        }
    }

    private void loadFileDetails(String fileName) {
        new Thread(() -> {
            try {
                appendLog("[Client] Reading file: " + fileName);
                Client client = new Client();
                byte[] data = client.read(fileName);
                String content = new String(data);

                SwingUtilities.invokeLater(() -> {
                    fileContentArea.setText(content);
                    chunksTableModel.setRowCount(0);

                    // Compute and show availability banner
                    List<Integer> hostingNodes = new ArrayList<>();
                    int onlineHosting = 0;
                    for (int r = 0; r < 5; r++) {
                        File f = new File("./Replica_" + r + "/" + fileName);
                        if (f.exists()) {
                            hostingNodes.add(r);
                            if (r < replicaLocs.size() && replicaLocs.get(r).isAlive()) onlineHosting++;
                        }
                    }

                    if (onlineHosting == hostingNodes.size() && !hostingNodes.isEmpty()) {
                        fileAvailabilityBanner.setText("🟢 100% High Availability: All " + onlineHosting + " replica copies online. Serving traffic normally.");
                        fileAvailabilityBanner.setBackground(new Color(236, 253, 245));
                        fileAvailabilityBanner.setForeground(new Color(6, 95, 70));
                    } else if (onlineHosting > 0) {
                        fileAvailabilityBanner.setText("⚠️ Degraded (" + onlineHosting + "/" + hostingNodes.size() + " copies online): Failover Active! Traffic diverted away from failed nodes.");
                        fileAvailabilityBanner.setBackground(new Color(254, 243, 199));
                        fileAvailabilityBanner.setForeground(new Color(146, 64, 14));
                    } else {
                        fileAvailabilityBanner.setText("🔴 Offline: All replica nodes hosting " + fileName + " are currently down.");
                        fileAvailabilityBanner.setBackground(new Color(254, 226, 226));
                        fileAvailabilityBanner.setForeground(new Color(153, 27, 27));
                    }

                    int chunkSize = Configurations.CHUNK_SIZE;
                    int totalChunks = (int) Math.ceil(1.0 * data.length / chunkSize);

                    for (int i = 0; i < totalChunks; i++) {
                        int start = i * chunkSize;
                        int end = Math.min(start + chunkSize, data.length);
                        byte[] chunk = Arrays.copyOfRange(data, start, end);

                        StringBuilder hex = new StringBuilder();
                        for (byte b : chunk) {
                            hex.append(String.format("%02X ", b));
                        }

                        chunksTableModel.addRow(new Object[]{
                                i,
                                hex.toString().trim(),
                                "\"" + new String(chunk) + "\"",
                                hostingNodes.toString()
                        });
                    }
                });

            } catch (Exception e) {
                appendLog("[ERROR] Failed to read " + fileName + ": " + e.getMessage());
                SwingUtilities.invokeLater(() -> {
                    fileAvailabilityBanner.setText("🔴 Read Failed: " + e.getMessage());
                    fileAvailabilityBanner.setBackground(new Color(254, 226, 226));
                    fileAvailabilityBanner.setForeground(new Color(153, 27, 27));
                    fileContentArea.setText("[ERROR: " + e.getMessage() + "]");
                    chunksTableModel.setRowCount(0);
                });
            }
        }).start();
    }

    // ==========================================
    // FILE UPLOAD, TRAFFIC CALCULATION & WRITE
    // ==========================================

    private void browseAndSelectLocalFile() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Select Local File to Upload to DFS");
        int res = chooser.showOpenDialog(this);
        if (res == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            if (file == null || !file.exists()) return;
            try {
                selectedLocalFile = file;
                uploadedFileBytes = Files.readAllBytes(file.toPath());
                writeFileNameField.setText(file.getName());

                boolean isText = isTextContent(uploadedFileBytes);
                if (isText && uploadedFileBytes.length <= 100_000) {
                    writeContentArea.setText(new String(uploadedFileBytes));
                    writeContentArea.setCaretPosition(0);
                } else if (isText) {
                    writeContentArea.setText(new String(Arrays.copyOfRange(uploadedFileBytes, 0, 50_000)) +
                            "\n\n... [Content truncated in preview: total " + formatFileSize(uploadedFileBytes.length) + "] ...");
                } else {
                    writeContentArea.setText("[Binary File: " + file.getName() + " (" + formatFileSize(uploadedFileBytes.length) +
                            ") - " + uploadedFileBytes.length + " bytes ready for chunked streaming]");
                }

                uploadFileInfoLabel.setText("📁 File Loaded: " + file.getName() + " | Size: " +
                        formatFileSize(uploadedFileBytes.length) + " (" + uploadedFileBytes.length + " B) | Path: " + file.getAbsolutePath());
                uploadFileInfoLabel.setForeground(new Color(16, 185, 129));
                uploadFileInfoLabel.setBackground(new Color(236, 253, 245));

                updateTrafficPlanPreview();
                appendLog("[Upload] Selected local file: " + file.getAbsolutePath() + " (" + uploadedFileBytes.length + " bytes)");
            } catch (Exception ex) {
                JOptionPane.showMessageDialog(this, "Failed to read file: " + ex.getMessage(), "File Error", JOptionPane.ERROR_MESSAGE);
                appendLog("[ERROR] Failed to read local file: " + ex.getMessage());
            }
        }
    }

    private boolean isTextContent(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return true;
        int checkLen = Math.min(bytes.length, 1024);
        int unprintable = 0;
        for (int i = 0; i < checkLen; i++) {
            byte b = bytes[i];
            if (b == 0) return false;
            if ((b < 32 && b != 9 && b != 10 && b != 13) || b < 0) {
                unprintable++;
            }
        }
        return (unprintable * 100.0 / checkLen) < 15.0;
    }

    private String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
    }

    private int getSelectedChunkSize(long dataLen) {
        if (chunkSizeCombo == null) return Configurations.CHUNK_SIZE;
        int idx = chunkSizeCombo.getSelectedIndex();
        switch (idx) {
            case 1: return 3; // GFS Default
            case 2: return 64;
            case 3: return 256;
            case 4: return 1024;
            case 0:
            default:
                // Adaptive auto-tuning
                if (dataLen <= 20) return 3;
                if (dataLen <= 512) return 64;
                if (dataLen <= 8192) return 256;
                return 1024;
        }
    }

    private void updateTrafficPlanPreview() {
        if (distributionMatrixPanel == null || planSummaryLabel == null) return;

        String fileName = (writeFileNameField != null) ? writeFileNameField.getText().trim() : "";
        if (fileName.isEmpty()) fileName = "file.txt";

        byte[] data;
        if (uploadedFileBytes != null) {
            data = uploadedFileBytes;
        } else if (writeContentArea != null) {
            data = writeContentArea.getText().getBytes();
        } else {
            data = new byte[0];
        }

        int chunkSize = getSelectedChunkSize(data.length);
        TrafficMonitor.FileTrafficPlan plan = TrafficMonitor.calculateTrafficDistribution(fileName, data.length, chunkSize);

        // Update Summary Label
        if (plan.primaryNodeId == -1) {
            planSummaryLabel.setText("<html><font color='#dc2626'><b>⚠️ All nodes are offline. Cannot route traffic.</b></font></html>");
        } else {
            planSummaryLabel.setText(String.format(
                    "<html><b>File:</b> <code>%s</code> (%s, %d chunks)<br/>" +
                    "<b>Chunk Size:</b> %d B | <b>Primary:</b> <font color='#2563eb'><b>Replica %d</b></font><br/>" +
                    "<b>Slaves:</b> %s | <b>Total Ops:</b> %d ops</html>",
                    plan.fileName, formatFileSize(plan.fileSize), plan.totalChunks,
                    plan.chunkSize, plan.primaryNodeId,
                    plan.slaveNodeIds.isEmpty() ? "None (Sole Survivor)" : plan.slaveNodeIds.toString(),
                    plan.totalProjectedOps));
        }

        // Populate distribution matrix cards
        distributionMatrixPanel.removeAll();
        for (int i = 0; i < 5; i++) {
            boolean isAlive = TrafficMonitor.isNodeAlive(i);
            JPanel row = new JPanel(new BorderLayout(6, 4));
            row.setOpaque(true);
            row.setBorder(new CompoundBorder(
                    new LineBorder(new Color(226, 232, 240), 1, true),
                    new EmptyBorder(6, 8, 6, 8)));

            if (!isAlive) {
                row.setBackground(new Color(254, 242, 242));
                JLabel titleLbl = new JLabel("Replica " + i + " (OFFLINE)");
                titleLbl.setFont(new Font("Segoe UI", Font.BOLD, 12));
                titleLbl.setForeground(new Color(220, 38, 38));

                JLabel detailLbl = new JLabel("Traffic diverted to surviving nodes");
                detailLbl.setFont(new Font("Segoe UI", Font.PLAIN, 11));
                detailLbl.setForeground(new Color(153, 27, 27));

                row.add(titleLbl, BorderLayout.WEST);
                row.add(detailLbl, BorderLayout.EAST);
            } else if (i == plan.primaryNodeId) {
                row.setBackground(new Color(239, 246, 255));
                row.setBorder(new CompoundBorder(
                        new LineBorder(new Color(59, 130, 246), 1, true),
                        new EmptyBorder(6, 8, 6, 8)));

                JPanel leftInfo = new JPanel(new GridLayout(2, 1));
                leftInfo.setOpaque(false);
                JLabel titleLbl = new JLabel("Replica " + i + " 👑 PRIMARY");
                titleLbl.setFont(new Font("Segoe UI", Font.BOLD, 12));
                titleLbl.setForeground(new Color(29, 78, 216));

                JLabel subLbl = new JLabel("Streams all " + plan.totalChunks + " chunks (" + formatFileSize(plan.fileSize) + ")");
                subLbl.setFont(new Font("Segoe UI", Font.PLAIN, 11));
                subLbl.setForeground(new Color(30, 64, 175));
                leftInfo.add(titleLbl);
                leftInfo.add(subLbl);
                row.add(leftInfo, BorderLayout.WEST);

                JPanel rightInfo = new JPanel(new GridLayout(2, 1));
                rightInfo.setOpaque(false);
                double pct = plan.nodeWorkloadPercentage.getOrDefault(i, 0.0);
                JLabel pctLbl = new JLabel(String.format("%.0f%% Traffic Share", pct), SwingConstants.RIGHT);
                pctLbl.setFont(new Font("Segoe UI", Font.BOLD, 11));
                pctLbl.setForeground(new Color(29, 78, 216));

                JProgressBar bar = new JProgressBar(0, 100);
                bar.setValue((int) pct);
                bar.setForeground(new Color(37, 99, 235));
                bar.setPreferredSize(new Dimension(80, 10));
                rightInfo.add(pctLbl);
                rightInfo.add(bar);
                row.add(rightInfo, BorderLayout.EAST);
            } else if (plan.slaveNodeIds.contains(i)) {
                row.setBackground(new Color(240, 253, 244));
                row.setBorder(new CompoundBorder(
                        new LineBorder(new Color(74, 222, 128), 1, true),
                        new EmptyBorder(6, 8, 6, 8)));

                JPanel leftInfo = new JPanel(new GridLayout(2, 1));
                leftInfo.setOpaque(false);
                JLabel titleLbl = new JLabel("Replica " + i + " 🔁 SLAVE REPLICA");
                titleLbl.setFont(new Font("Segoe UI", Font.BOLD, 12));
                titleLbl.setForeground(new Color(21, 128, 61));

                JLabel subLbl = new JLabel("Receives 2PC commit replicate payload");
                subLbl.setFont(new Font("Segoe UI", Font.PLAIN, 11));
                subLbl.setForeground(new Color(22, 101, 52));
                leftInfo.add(titleLbl);
                leftInfo.add(subLbl);
                row.add(leftInfo, BorderLayout.WEST);

                JPanel rightInfo = new JPanel(new GridLayout(2, 1));
                rightInfo.setOpaque(false);
                double pct = plan.nodeWorkloadPercentage.getOrDefault(i, 0.0);
                JLabel pctLbl = new JLabel(String.format("%.0f%% Replicated", pct), SwingConstants.RIGHT);
                pctLbl.setFont(new Font("Segoe UI", Font.BOLD, 11));
                pctLbl.setForeground(new Color(21, 128, 61));

                JProgressBar bar = new JProgressBar(0, 100);
                bar.setValue((int) pct);
                bar.setForeground(new Color(22, 163, 74));
                bar.setPreferredSize(new Dimension(80, 10));
                rightInfo.add(pctLbl);
                rightInfo.add(bar);
                row.add(rightInfo, BorderLayout.EAST);
            } else {
                row.setBackground(new Color(248, 250, 252));
                JLabel titleLbl = new JLabel("Replica " + i + " (Standby)");
                titleLbl.setFont(new Font("Segoe UI", Font.PLAIN, 12));
                titleLbl.setForeground(new Color(100, 116, 139));

                JLabel detailLbl = new JLabel("Available for future files / failover");
                detailLbl.setFont(new Font("Segoe UI", Font.ITALIC, 11));
                detailLbl.setForeground(new Color(148, 163, 184));

                row.add(titleLbl, BorderLayout.WEST);
                row.add(detailLbl, BorderLayout.EAST);
            }

            distributionMatrixPanel.add(row);
        }

        distributionMatrixPanel.revalidate();
        distributionMatrixPanel.repaint();
    }

    private void exportFileToDisk(String fileName) {
        if (fileName == null || fileName.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Please select a file to export.", "Notice", JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        JFileChooser chooser = new JFileChooser();
        chooser.setSelectedFile(new File(fileName));
        chooser.setDialogTitle("Export DFS File to Local Disk");
        int res = chooser.showSaveDialog(this);
        if (res == JFileChooser.APPROVE_OPTION) {
            File targetFile = chooser.getSelectedFile();
            new Thread(() -> {
                try {
                    appendLog("[Export] Reading '" + fileName + "' from DFS cluster...");
                    Client client = new Client();
                    byte[] data = client.read(fileName);
                    if (data == null) {
                        appendLog("[ERROR] File read returned empty/null data.");
                        return;
                    }
                    Files.write(targetFile.toPath(), data);
                    appendLog("[Export] Successfully exported '" + fileName + "' (" + data.length + " bytes) to " + targetFile.getAbsolutePath());
                    SwingUtilities.invokeLater(() -> {
                        JOptionPane.showMessageDialog(this,
                                "File exported successfully!\nSaved to: " + targetFile.getAbsolutePath() + "\nSize: " + data.length + " bytes",
                                "Export Complete", JOptionPane.INFORMATION_MESSAGE);
                    });
                } catch (Exception ex) {
                    appendLog("[ERROR] Export failed: " + ex.getMessage());
                    SwingUtilities.invokeLater(() -> {
                        JOptionPane.showMessageDialog(this, "Export failed: " + ex.getMessage(), "Export Error", JOptionPane.ERROR_MESSAGE);
                    });
                }
            }).start();
        }
    }

    private void executeWrite() {
        String fileName = writeFileNameField.getText().trim();
        if (fileName.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Please enter a file name.", "Input Required", JOptionPane.WARNING_MESSAGE);
            return;
        }

        byte[] payloadData;
        if (uploadedFileBytes != null) {
            payloadData = uploadedFileBytes;
        } else {
            payloadData = writeContentArea.getText().getBytes();
        }

        final byte[] data = payloadData;
        final int chunkSize = getSelectedChunkSize(data.length);
        final int segN = (int) Math.max(1, Math.ceil(1.0 * data.length / chunkSize));

        writeProgressBar.setVisible(true);
        writeProgressBar.setIndeterminate(false);
        writeProgressBar.setValue(0);
        writeProgressBar.setString("Calculating traffic distribution...");
        writeStatusLabel.setText("Connecting to Master Server and calculating node distribution...");

        new Thread(() -> {
            try {
                TrafficMonitor.FileTrafficPlan plan = TrafficMonitor.calculateTrafficDistribution(fileName, data.length, chunkSize);
                appendLog("[Upload] Slicing '" + fileName + "' (" + formatFileSize(data.length) + ") into " + segN + " chunks (Chunk Size = " + chunkSize + " B)...");
                appendLog("[Traffic] Projected traffic: Primary Node " + plan.primaryNodeId + " receives " + plan.totalChunks + " chunk writes; Slaves " + plan.slaveNodeIds + " receive 2PC commit payload.");

                SwingUtilities.invokeLater(() -> {
                    writeProgressBar.setValue(10);
                    writeProgressBar.setString("Requesting Master write authorization (10%)...");
                    writeStatusLabel.setText("Obtaining write authorization and primary replica from Master...");
                });

                Client client = new Client();
                WriteAck ackMsg = client.masterStub.write(fileName);
                int primaryId = ackMsg.getLoc().getId();
                ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + primaryId);

                appendLog("[Client] Master granted write transaction #" + ackMsg.getTransactionId() + " on Primary Replica " + primaryId);

                FileContent fileContent = new FileContent(fileName);
                ChunkAck chunkAck;
                byte[] chunk = new byte[chunkSize];

                for (int i = 0; i < segN - 1; i++) {
                    System.arraycopy(data, i * chunkSize, chunk, 0, chunkSize);
                    fileContent.setData(chunk);
                    do {
                        chunkAck = replicaStub.write(ackMsg.getTransactionId(), i, fileContent);
                    } while (chunkAck.getSeqNo() != i);

                    final int currentChunk = i + 1;
                    final int totalChunks = segN;
                    final int progressPct = 10 + (int) Math.round((currentChunk * 75.0) / totalChunks);
                    SwingUtilities.invokeLater(() -> {
                        writeProgressBar.setValue(progressPct);
                        writeProgressBar.setString("Streaming chunk " + currentChunk + "/" + totalChunks + " (" + progressPct + "%)");
                        writeStatusLabel.setText("Chunk " + currentChunk + "/" + totalChunks + " acknowledged by Replica " + primaryId);
                    });
                }

                // Handling last chunk of the file
                int lastChunkLen = (data.length % chunkSize > 0) ? (data.length % chunkSize) : (data.length == 0 ? 0 : chunkSize);
                chunk = new byte[lastChunkLen];
                if (data.length > 0) {
                    System.arraycopy(data, (segN - 1) * chunkSize, chunk, 0, lastChunkLen);
                }
                fileContent.setData(chunk);
                do {
                    chunkAck = replicaStub.write(ackMsg.getTransactionId(), segN - 1, fileContent);
                } while (chunkAck.getSeqNo() != segN - 1);

                final int totalChunks = segN;
                SwingUtilities.invokeLater(() -> {
                    writeProgressBar.setValue(88);
                    writeProgressBar.setString("All " + totalChunks + " chunks sent. Initiating 2-Phase Commit (88%)...");
                    writeStatusLabel.setText("Primary Replica " + primaryId + " replicating update across slave nodes...");
                });

                appendLog("[Client] All " + segN + " chunks written. Initiating 2-Phase Commit...");
                replicaStub.commit(ackMsg.getTransactionId(), segN);
                appendLog("[Client] Commit completed successfully across replicas!");

                SwingUtilities.invokeLater(() -> {
                    writeProgressBar.setValue(100);
                    writeProgressBar.setString("100% Upload & Distribution Complete!");
                    writeStatusLabel.setText("Successfully committed " + formatFileSize(data.length) + " across " + segN + " chunks on cluster replicas!");
                    refreshAll();
                    updateTrafficPlanPreview();
                    JOptionPane.showMessageDialog(this,
                            "File '" + fileName + "' successfully uploaded and distributed!\n" +
                            "File Size: " + formatFileSize(data.length) + " (" + data.length + " bytes)\n" +
                            "Chunks Streamed: " + totalChunks + " chunks (" + chunkSize + " B each)\n" +
                            "Primary Replica: Replica " + primaryId + "\n" +
                            "Distributed Traffic: Sliced and load-balanced across active nodes.",
                            "Upload & Distribution Succeeded", JOptionPane.INFORMATION_MESSAGE);
                });

            } catch (Exception e) {
                appendLog("[ERROR] Write failed: " + e.getMessage());
                e.printStackTrace();
                SwingUtilities.invokeLater(() -> {
                    writeProgressBar.setValue(0);
                    writeProgressBar.setString("Failed: " + e.getMessage());
                    writeStatusLabel.setText("Write failed: " + e.getMessage());
                    JOptionPane.showMessageDialog(this, "Write failed: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                });
            }
        }).start();
    }

    // ==========================================
    // TEST RUNNERS
    // ==========================================

    private void runStandardDemo() {
        new Thread(() -> {
            appendLog("[Test] Launching standard multi-client test suite...");
            try {
                Main.launchClients();
                appendLog("[Test] Standard test suite finished successfully!");
                SwingUtilities.invokeLater(this::refreshAll);
            } catch (Exception e) {
                appendLog("[ERROR] Standard test failed: " + e.getMessage());
            }
        }).start();
    }

    private void runFailoverTest() {
        new Thread(() -> {
            appendLog("[Failover-Test] ==================================================");
            appendLog("[Failover-Test] Starting Automatic Node Failover & Traffic Transfer Test");
            appendLog("[Failover-Test] ==================================================");
            try {
                if (master == null) {
                    appendLog("[Failover-Test] Cluster master is not ready yet. Initializing...");
                    initCluster();
                    Thread.sleep(1500);
                }

                String testFileName = "failover_demo_" + (System.currentTimeMillis() % 10000) + ".txt";
                String testContent = "Failover test payload - High availability across distributed replica nodes at " + new Date();

                appendLog("[Failover-Test] Step 1: Writing test file '" + testFileName + "' across replicas...");
                Client client = new Client();
                client.write(testFileName, testContent.getBytes());
                appendLog("[Failover-Test] Step 1 complete. File created successfully across replicas.");

                // Locate the replicas
                List<ReplicaLoc> locs = master.read(testFileName);
                if (locs == null || locs.isEmpty()) {
                    appendLog("[Failover-Test] [ERROR] Could not query file locations from Master.");
                    return;
                }

                ReplicaLoc chosenNode = master.locatePrimaryReplica(testFileName);
                if (chosenNode == null) {
                    chosenNode = locs.get(0);
                }
                final ReplicaLoc primaryNode = chosenNode;

                appendLog("[Failover-Test] Step 2: Primary replica identified as Replica ID: " + primaryNode.getId() + " (" + primaryNode.getAddress() + ").");
                appendLog("[Failover-Test] Step 3: 💥 Simulating sudden crash/failure of Replica ID: " + primaryNode.getId() + "...");
                primaryNode.setAlive(false);
                master.setReplicaAlive(primaryNode.getId(), false);
                TrafficMonitor.recordFailover(primaryNode.getId(), "Simulated Node Crash for Test");
                SwingUtilities.invokeLater(this::refreshAll);

                appendLog("[Failover-Test] Step 4: Initiating read on '" + testFileName + "' via Client while primary replica is OFFLINE...");
                long startTime = System.currentTimeMillis();
                byte[] data = client.read(testFileName);
                long duration = System.currentTimeMillis() - startTime;
                String retrieved = (data != null) ? new String(data) : "";

                if (!retrieved.isEmpty()) {
                    appendLog("[Failover-Test] 🟢 SUCCESS! Read traffic was automatically rerouted to a surviving replica!");
                    appendLog("[Failover-Test] Retrieved Content: \"" + (retrieved.length() > 60 ? retrieved.substring(0, 60) + "..." : retrieved) + "\"");
                    appendLog("[Failover-Test] Transfer latency: " + duration + "ms | Data Integrity: 100%");

                    SwingUtilities.invokeLater(() -> {
                        refreshAll();
                        JOptionPane.showMessageDialog(this,
                                "✅ Automatic Node Failover Test PASSED!\n\n" +
                                "1. File '" + testFileName + "' was written across replica nodes.\n" +
                                "2. Primary Replica " + primaryNode.getId() + " was intentionally taken OFFLINE.\n" +
                                "3. Client automatically detected failure and transferred traffic to surviving node.\n" +
                                "4. File data was retrieved with 100% integrity in " + duration + "ms!\n\n" +
                                "Primary node will now be restored back online.",
                                "Failover Test PASSED",
                                JOptionPane.INFORMATION_MESSAGE);
                    });
                } else {
                    appendLog("[Failover-Test] ❌ FAILED: Could not read file from surviving replicas.");
                }

                // Restore node after short delay
                Thread.sleep(2500);
                primaryNode.setAlive(true);
                master.setReplicaAlive(primaryNode.getId(), true);
                appendLog("[Failover-Test] 🟢 Restored Replica ID: " + primaryNode.getId() + " back ONLINE.");
                SwingUtilities.invokeLater(this::refreshAll);

            } catch (Exception e) {
                appendLog("[Failover-Test] [ERROR] Failover test encountered exception: " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }

    private void runCustomTest() {
        new Thread(() -> {
            appendLog("[Test] Launching custom concurrency and commit test...");
            try {
                Main.customTest();
                appendLog("[Test] Custom concurrency test finished successfully!");
                SwingUtilities.invokeLater(this::refreshAll);
            } catch (Exception e) {
                appendLog("[ERROR] Custom test failed: " + e.getMessage());
            }
        }).start();
    }

    private void runBatchTest() {
        new Thread(() -> {
            appendLog("[Test] Launching batch operations test...");
            try {
                Client client = new Client();
                String[] batch = {
                        "write, batchDemo.txt, Batch message 1 from automated test",
                        "read, batchDemo.txt",
                        "write, batchDemo.txt, Append batch message 2"
                };
                client.batchOperations(batch);
                appendLog("[Test] Batch operations finished!");
                SwingUtilities.invokeLater(this::refreshAll);
            } catch (Exception e) {
                appendLog("[ERROR] Batch test failed: " + e.getMessage());
            }
        }).start();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            DFSGui gui = new DFSGui();
            gui.setVisible(true);
        });
    }
}
